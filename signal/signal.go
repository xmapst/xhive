// Package signal 提供进程信号的注册、分发和停止监听能力。
// Manager 由 app 包集成，也可通过 NewManager 独立使用。
package signal

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
)

// Trap 是信号处理函数类型。
// 处理函数在并发 goroutine 中执行，panic 会被捕获并记录日志，不影响同一信号的其他处理器。
type Trap func()

// Manager 管理进程信号的注册与分发。
//
// 同一信号支持注册多个处理器，收到信号时并发调用、等待全部完成。
// SIGINT/SIGTERM 由框架固定用于优雅关闭；SIGKILL 不可捕获，仅作为保留信号禁止外部注册。
type Manager struct {
	signals         map[os.Signal][]Trap
	sigCh           chan os.Signal // 由 Start 创建，nil 表示分发器尚未启动
	reservedSignals map[os.Signal]struct{}
	sync.RWMutex
}

// NewManager 创建 Manager，预置框架保留信号。
// SIGINT/SIGTERM 可被捕获并用于优雅关闭；SIGKILL 不可捕获，仅用于禁止业务注册。
func NewManager() *Manager {
	return &Manager{
		signals: make(map[os.Signal][]Trap),
		reservedSignals: map[os.Signal]struct{}{
			syscall.SIGINT:  {},
			syscall.SIGKILL: {},
			syscall.SIGTERM: {},
		},
	}
}

// Register 追加信号处理器，并发安全。
// 同一信号可多次注册；收到信号时会为每个处理器启动 goroutine 并等待全部完成，不保证完成顺序。
// SIGINT/SIGTERM 为可捕获的框架保留信号；SIGKILL 不可捕获，也作为保留信号禁止业务注册。
func (m *Manager) Register(trap Trap, sigs ...os.Signal) error {
	m.Lock()
	defer m.Unlock()

	for _, sig := range sigs {
		if _, reserved := m.reservedSignals[sig]; reserved {
			return fmt.Errorf("signal %s is reserved by the framework; external registration is not permitted", sig)
		}
	}

	for _, sig := range sigs {
		m.signals[sig] = append(m.signals[sig], trap)
	}
	// 若分发器已启动（sigCh 已创建），追加监听新信号，无需重建 channel
	if m.sigCh != nil {
		signal.Notify(m.sigCh, sigs...)
	}
	return nil
}

// Start 注册框架默认信号处理器并启动信号分发 goroutine，只在 app.Run 内调用一次。
//
// SIGINT/SIGTERM 固定绑定到 stopFn 触发优雅关闭；SIGKILL 不可捕获，虽保留在内部表中但不会触发处理器。
// SIGHUP 仅在业务层未注册处理器时才追加默认行为（记录日志继续运行）。
// sigCh 在锁内赋值后，后续 Register 调用可安全追加新信号到同一 channel。
func (m *Manager) Start(stopFn func()) {
	m.Lock()

	m.signals[syscall.SIGINT] = []Trap{stopFn}
	m.signals[syscall.SIGKILL] = []Trap{stopFn}
	m.signals[syscall.SIGTERM] = []Trap{stopFn}

	if _, ok := m.signals[syscall.SIGHUP]; !ok {
		m.signals[syscall.SIGHUP] = []Trap{func() {
			slog.Info("received sighup signal")
		}}
	}

	allSigs := make([]os.Signal, 0, len(m.signals))
	for sig := range m.signals {
		allSigs = append(allSigs, sig)
	}
	sigCh := make(chan os.Signal, 8)
	signal.Notify(sigCh, allSigs...)
	m.sigCh = sigCh

	m.Unlock()

	// 每个信号的处理器并发执行，取读锁后立即释放，避免在 channel 阻塞期间持锁与 Register 的写锁死锁。
	go func() {
		for sig := range sigCh {
			slog.Info("signal received", slog.Any("signal", sig))
			m.RLock()
			traps := m.signals[sig]
			m.RUnlock()
			var wg sync.WaitGroup
			for _, trap := range traps {
				wg.Go(func() {
					defer func() {
						if r := recover(); r != nil {
							slog.Error("signal handler panicked", slog.Any("signal", sig), slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
						}
					}()
					trap()
				})
			}
			wg.Wait()
		}
	}()
}

// Stop 注销 OS 信号监听并关闭分发 goroutine，在 app 关闭后调用。
// signal.Stop 保证此后不再有新信号写入 sigCh，close 后分发 goroutine 的 range 循环安全退出。
func (m *Manager) Stop() {
	m.Lock()
	defer m.Unlock()
	if m.sigCh == nil {
		return
	}
	signal.Stop(m.sigCh)
	close(m.sigCh)
	m.sigCh = nil
}
