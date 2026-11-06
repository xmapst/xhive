package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// StopPreparer drains producers and outstanding work while Serve remains live.
// It must also join work if Serve already exited, and be retryable after an error
// or context expiry. Dependencies remain live until preparation succeeds.
type StopPreparer interface {
	PrepareStop(context.Context) error
}

// Quiescer freezes static controllers before any dynamic dependency is stopped.
// Shared services should drain in PrepareStop rather than Quiesce.
type Quiescer interface {
	Quiesce(context.Context) error
}

// Ready closes after all static modules are ready and the application is running.
func (a *App) Ready() <-chan struct{} { return a.ready }

// RunContext runs an independent application without registering process signals.
// Cancellation starts ordered shutdown and waits for every live owner to release
// its resources; a shutdown warning timeout does not release its dependencies.
func (a *App) RunContext(ctx context.Context, mods ...IModule) error {
	return a.run(ctx, false, mods...)
}

// WithOSThreadLock controls whether each module binds a dedicated OS thread.
// The default is true; pure Go services can opt out.
func WithOSThreadLock(enabled bool) Option {
	return func(a *App) { a.lockOSThread = enabled }
}

// WithPanicExit controls process termination on a static module panic.
// When false, Run and RunContext return a failure after ordered cleanup.
func WithPanicExit(enabled bool) Option {
	return func(a *App) { a.panicExit = enabled }
}

// WithStartupTimeout bounds waiting for a module's Ready signal.
func WithStartupTimeout(timeout time.Duration) Option {
	return func(a *App) {
		if timeout > 0 {
			a.startupTimeout = timeout
		}
	}
}

// RemoveDynamicModuleContext retires one module and waits for complete cleanup.
// A canceled waiter leaves cleanup running and the name reserved. Repeated calls
// join the same cleanup; an already absent module is a successful no-op.
func (a *App) RemoveDynamicModuleContext(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value, exists := a.dynamicModules.Load(name)
	if !exists {
		return nil
	}
	wrapper, ok := value.(*moduleWrapper)
	if !ok {
		return fmt.Errorf("invalid dynamic module %q", name)
	}
	if !wrapper.visible.Load() && !wrapper.retiring.Load() {
		return fmt.Errorf("dynamic module %q is not ready", name)
	}
	a.beginShutdown(wrapper)
	select {
	case <-wrapper.shutdownDone:
		return wrapper.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *App) awaitReady(wrapper *moduleWrapper) bool {
	timer := time.NewTimer(a.startupTimeout)
	defer timer.Stop()
	select {
	case <-wrapper.Ready():
		return true
	case <-wrapper.exited:
		select {
		case <-wrapper.Ready():
			return true
		default:
			return false
		}
	case <-timer.C:
		slog.Error("module startup timed out", slog.String("module", wrapper.Name()))
		return false
	}
}

func (a *App) initializeModule(wrapper *moduleWrapper) (err error) {
	if !wrapper.advanceLifecycle(lifecycleRegistered, lifecycleIniting) {
		return fmt.Errorf("module %q was retired before initialization", wrapper.Name())
	}
	defer close(wrapper.initDone)
	defer func() {
		if value := recover(); value != nil {
			if a.panicExit {
				panic(value)
			}
			err = fmt.Errorf("module %q initialization panicked", wrapper.Name())
		}
		wrapper.initErr = err
		if err != nil {
			wrapper.advanceLifecycle(lifecycleIniting, lifecycleInitFailed)
		}
	}()
	if err = wrapper.OnInit(); err != nil {
		return err
	}
	if !wrapper.advanceLifecycle(lifecycleIniting, lifecycleInited) {
		// Shutdown owns cleanup and observes initErr after initDone closes.
		return nil
	}
	return nil
}

func (a *App) retryPreparation(wrapper *moduleWrapper, prepare func(context.Context) error) {
	var lastWarning time.Time
	for {
		ctx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
		err := a.prepareModule(ctx, wrapper, prepare)
		cancel()
		if err == nil {
			return
		}
		if lastWarning.IsZero() || time.Since(lastWarning) >= a.shutdownTimeout {
			slog.Warn("module preparation deferred", slog.String("module", wrapper.Name()), slog.Any("error", err))
			lastWarning = time.Now()
		}
		time.Sleep(min(100*time.Millisecond, a.shutdownTimeout))
	}
}

func (a *App) prepareModule(ctx context.Context, wrapper *moduleWrapper, prepare func(context.Context) error) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("module %q stop preparation panicked", wrapper.Name())
		}
	}()
	return prepare(ctx)
}

func (a *App) moduleFailure(failure error) {
	select {
	case a.failures <- failure:
	default:
	}
}

func (a *App) waitShutdown(wrapper *moduleWrapper) error {
	a.beginShutdown(wrapper)
	for {
		timer := time.NewTimer(a.shutdownTimeout)
		select {
		case <-wrapper.shutdownDone:
			timer.Stop()
			return wrapper.shutdownErr
		case <-timer.C:
			slog.Warn("module shutdown waiting", slog.String("module", wrapper.Name()))
		}
	}
}

func (a *App) beginShutdown(wrapper *moduleWrapper) {
	wrapper.shutdownOnce.Do(func() {
		wrapper.retiring.Store(true)
		wrapper.visible.Store(false)
		go func() {
			wrapper.shutdownErr = a.finishShutdown(wrapper)
			a.dynamicModules.CompareAndDelete(wrapper.Name(), wrapper)
			close(wrapper.shutdownDone)
		}()
	})
}

func (a *App) finishShutdown(wrapper *moduleWrapper) error {
	state := wrapper.claimShutdown()
	if state == lifecycleIniting {
		// OnInit owns its mutable fields until this barrier. Never destroy them
		// concurrently or release the name just because a waiter timed out.
		for {
			timer := time.NewTimer(a.shutdownTimeout)
			select {
			case <-wrapper.initDone:
				timer.Stop()
				state = lifecycleInited
				if wrapper.initErr != nil {
					state = lifecycleInitFailed
				}
			case <-timer.C:
				slog.Warn("module initialization shutdown waiting", slog.String("module", wrapper.Name()))
				continue
			}
			break
		}
	}
	if state == lifecycleServing {
		ready := a.awaitReady(wrapper)
		if preparer, ok := wrapper.IModule.(StopPreparer); ok && ready {
			a.retryPreparation(wrapper, preparer.PrepareStop)
		}
		wrapper.cancel()
		for !a.waitModuleExit(wrapper) {
			// Keep ownership and dependencies until the actual Serve returns.
		}
	} else {
		wrapper.cancel()
	}
	a.closeIdleServer(wrapper)
	var err error
	if state == lifecycleServing || state == lifecycleInited {
		err = errors.Join(a.destroyModule(wrapper), a.closeModule(wrapper))
	}
	wrapper.setLifecycle(lifecycleDestroyed)
	return err
}
