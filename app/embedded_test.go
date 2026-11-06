package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type drainingTestModule struct {
	*testModule
	requests    chan chan struct{}
	preparing   chan struct{}
	release     chan struct{}
	prepareOnce sync.Once
	servedDone  chan struct{}
	quiesce     func()
	earlyExit   bool
}

func newDrainingTestModule(name string) *drainingTestModule {
	release := make(chan struct{})
	close(release)
	return &drainingTestModule{testModule: newTestModule(name), requests: make(chan chan struct{}), preparing: make(chan struct{}), release: release, servedDone: make(chan struct{})}
}

func (m *drainingTestModule) Serve(ctx context.Context) {
	defer close(m.servedDone)
	m.serveCalls.Add(1)
	m.readyOnce.Do(func() { close(m.ready) })
	if m.earlyExit {
		return
	}
	for {
		select {
		case done := <-m.requests:
			close(done)
		case <-ctx.Done():
			return
		}
	}
}

func (m *drainingTestModule) Quiesce(context.Context) error {
	if m.quiesce != nil {
		m.quiesce()
	}
	return nil
}

func (m *drainingTestModule) PrepareStop(ctx context.Context) error {
	select {
	case <-m.servedDone:
		return nil
	default:
	}
	m.prepareOnce.Do(func() { close(m.preparing) })
	// The module's event loop must remain available throughout business drain.
	done := make(chan struct{})
	select {
	case m.requests <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-m.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func awaitAppState(t *testing.T, a *App, state int32) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for a.State() != state {
		select {
		case <-deadline.C:
			t.Fatalf("application state = %d, want %d", a.State(), state)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestRunContextQuiescesControllersBeforeDynamicDrain(t *testing.T) {
	a := New(WithShutdownTimeout(100*time.Millisecond), WithOSThreadLock(false))
	controller := newDrainingTestModule("controller")
	var quiesced bool
	controller.quiesce = func() { quiesced = true }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.RunContext(ctx, controller) }()
	awaitAppState(t, a, StateRun)
	dynamic := newDrainingTestModule("pipeline")
	if _, err := a.AddDynamicModules(dynamic); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-dynamic.preparing:
		if !quiesced {
			t.Fatal("dynamic drain preceded controller quiescence")
		}
	case <-time.After(time.Second):
		t.Fatal("dynamic module was not prepared")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not stop the application")
	}
	assertModuleCalls(t, controller.testModule, [4]int32{1, 1, 1, 1})
	assertModuleCalls(t, dynamic.testModule, [4]int32{1, 1, 1, 1})
}

func TestDynamicRemovalTimeoutRetainsNameUntilCleanup(t *testing.T) {
	a := newTestApp(t)
	m := newDrainingTestModule("pipeline")
	m.release = make(chan struct{})
	release := sync.OnceFunc(func() { close(m.release) })
	t.Cleanup(release)
	if _, err := a.AddDynamicModules(m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := a.RemoveDynamicModuleContext(ctx, m.Name()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("removal = %v, want deadline exceeded", err)
	}
	if m.destroyCalls.Load() != 0 || m.closeCalls.Load() != 0 {
		t.Fatal("live module was destroyed")
	}
	replacement := newTestModule(m.Name())
	if _, err := a.AddDynamicModules(replacement); err == nil {
		t.Fatal("timed-out removal released the module name")
	}
	assertModuleCalls(t, replacement, [4]int32{})
	release()
	bounded, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := a.RemoveDynamicModuleContext(bounded, m.Name()); err != nil {
		t.Fatal(err)
	}
	assertModuleCalls(t, m.testModule, [4]int32{1, 1, 1, 1})
	if _, err := a.AddDynamicModules(replacement); err != nil {
		t.Fatal("completed removal still owns the name", err)
	}
}

func TestRunContextKeepsDependenciesDuringTimedOutDrain(t *testing.T) {
	a := New(WithShutdownTimeout(20*time.Millisecond), WithOSThreadLock(false))
	dependency := newDrainingTestModule("reporter")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.RunContext(ctx, dependency) }()
	awaitAppState(t, a, StateRun)
	dynamic := newDrainingTestModule("pipeline")
	dynamic.release = make(chan struct{})
	release := sync.OnceFunc(func() { close(dynamic.release) })
	t.Cleanup(release)
	if _, err := a.AddDynamicModules(dynamic); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-dynamic.preparing
	select {
	case err := <-done:
		t.Fatal("application returned with a live module", err)
	case <-time.After(70 * time.Millisecond):
	}
	if dependency.closeCalls.Load() != 0 {
		t.Fatal("dependency closed before dynamic drain completed")
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not resume after release")
	}
}

func TestRunContextReturnsUnexpectedStaticExit(t *testing.T) {
	a := New(WithOSThreadLock(false))
	mod := newDrainingTestModule("early")
	mod.earlyExit = true
	m := mod.testModule
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := a.RunContext(ctx, mod)
	if err == nil || !strings.Contains(err.Error(), m.Name()) {
		t.Fatalf("static exit = %v, want module failure", err)
	}
	assertModuleCalls(t, m, [4]int32{1, 1, 1, 1})
}

func TestRunContextCancellationWaitsForLateInitialization(t *testing.T) {
	a := New(WithShutdownTimeout(20*time.Millisecond), WithOSThreadLock(false))
	a.startupSettle = 5 * time.Millisecond
	m := newTestModule("initializing")
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	m.initFn = func() error { close(entered); <-release; return nil }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.RunContext(ctx, m) }()
	<-entered
	cancel()
	select {
	case <-done:
		t.Fatal("application returned during active initialization")
	case <-time.After(50 * time.Millisecond):
	}
	if m.destroyCalls.Load() != 0 {
		t.Fatal("destroy ran concurrently with initialization")
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("late initialization did not join shutdown")
	}
	assertModuleCalls(t, m, [4]int32{1, 0, 1, 1})
}

type delayedExitTestModule struct {
	*testModule
	release chan struct{}
}

func (m *delayedExitTestModule) Serve(ctx context.Context) {
	m.serveCalls.Add(1)
	<-ctx.Done()
	<-m.release
}

func TestDynamicStartupTimeoutKeepsReservationUntilExit(t *testing.T) {
	a := New(WithStartupTimeout(10*time.Millisecond), WithShutdownTimeout(20*time.Millisecond), WithOSThreadLock(false))
	m := &delayedExitTestModule{testModule: newTestModule("not-ready"), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(m.release) })
	t.Cleanup(unblock)
	if _, err := a.AddDynamicModules(m); err == nil {
		t.Fatal("startup without Ready was accepted")
	}
	if m.destroyCalls.Load() != 0 {
		t.Fatal("startup timeout destroyed an active Serve")
	}
	if _, err := a.AddDynamicModules(newTestModule(m.Name())); err == nil {
		t.Fatal("startup timeout released a live owner's name")
	}
	unblock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := a.RemoveDynamicModuleContext(ctx, m.Name()); err != nil {
		t.Fatal(err)
	}
	assertModuleCalls(t, m.testModule, [4]int32{1, 1, 1, 1})
}

type retryingPreparationModule struct {
	*drainingTestModule
	attempts int
}

func (m *retryingPreparationModule) PrepareStop(ctx context.Context) error {
	m.attempts++
	if m.attempts == 1 {
		panic("injected preparation failure")
	}
	return m.drainingTestModule.PrepareStop(ctx)
}

func TestRunContextRetriesPreparationPanicWithoutLosingOwner(t *testing.T) {
	a := New(WithShutdownTimeout(20*time.Millisecond), WithOSThreadLock(false), WithPanicExit(false))
	mod := &retryingPreparationModule{drainingTestModule: newDrainingTestModule("retrying")}
	quiesceAttempts := 0
	mod.quiesce = func() {
		quiesceAttempts++
		if quiesceAttempts == 1 {
			panic("injected quiesce failure")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.RunContext(ctx, mod) }()
	awaitAppState(t, a, StateRun)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("preparation retry did not finish")
	}
	if quiesceAttempts != 2 || mod.attempts != 2 {
		t.Fatalf("quiesce/prepare attempts = %d/%d, want 2/2", quiesceAttempts, mod.attempts)
	}
	assertModuleCalls(t, mod.testModule, [4]int32{1, 1, 1, 1})
}

type panickingStaticModule struct{ *testModule }

func (m *panickingStaticModule) Serve(context.Context) {
	m.serveCalls.Add(1)
	m.readyOnce.Do(func() { close(m.ready) })
	panic("injected static failure")
}

func TestRunContextReportsPanicsWhenProcessExitDisabled(t *testing.T) {
	for _, phase := range []string{"initialization", "serving"} {
		t.Run(phase, func(t *testing.T) {
			a := New(WithOSThreadLock(false), WithPanicExit(false))
			module := &panickingStaticModule{testModule: newTestModule(phase)}
			want := [4]int32{1, 1, 1, 1}
			if phase == "initialization" {
				module.initFn = func() error { panic("injected initialization failure") }
				want = [4]int32{1, 0, 0, 0}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := a.RunContext(ctx, module); err == nil || !strings.Contains(err.Error(), phase) {
				t.Fatal("static panic was not returned", err)
			}
			assertModuleCalls(t, module.testModule, want)
		})
	}
}
