package xhive

import (
    "fmt"
    "log/slog"
    "os"
    "os/signal"
    "runtime/debug"
    "sync"
    "syscall"
)

// SignalTrap 是信号处理函数类型。
// 处理函数在并发 goroutine 中执行，panic 会被捕获并记录日志，不影响同一信号的其他处理器。
type SignalTrap func()

// SignalManager 管理进程信号的注册与分发。
//
// 同一信号支持注册多个处理器，收到信号时并发调用、等待全部完成。
// SIGINT/SIGKILL/SIGTERM 由框架独占（固定触发优雅关闭），不允许外部注册。
type SignalManager struct {
    sync.RWMutex
    signals         map[os.Signal][]SignalTrap
    sigCh           chan os.Signal // 由 Start 创建，nil 表示分发器尚未启动
    reservedSignals map[os.Signal]struct{}
}

// NewSignalManager 创建 SignalManager，预置 SIGINT/SIGKILL/SIGTERM 为框架保留信号。
func NewSignalManager() *SignalManager {
    return &SignalManager{
        signals: make(map[os.Signal][]SignalTrap),
        reservedSignals: map[os.Signal]struct{}{
            syscall.SIGINT:  {},
            syscall.SIGKILL: {},
            syscall.SIGTERM: {},
        },
    }
}

// Register 追加信号处理器，并发安全。
// 同一信号可多次注册，处理器按注册顺序并发执行。
// SIGINT/SIGKILL/SIGTERM 为框架保留信号，传入时返回错误。
func (sm *SignalManager) Register(trap SignalTrap, sigs ...os.Signal) error {
    sm.Lock()
    defer sm.Unlock()
    
    for _, sig := range sigs {
        if _, reserved := sm.reservedSignals[sig]; reserved {
            return fmt.Errorf("signal %s is reserved by the framework; external registration is not permitted", sig)
        }
    }
    
    for _, sig := range sigs {
        sm.signals[sig] = append(sm.signals[sig], trap)
    }
    // 若分发器已启动（sigCh 已创建），追加监听新信号，无需重建 channel
    if sm.sigCh != nil {
        signal.Notify(sm.sigCh, sigs...)
    }
    return nil
}

// Start 注册框架默认信号处理器并启动信号分发 goroutine，只在 app.Run 内调用一次。
//
// SIGINT/SIGKILL/SIGTERM 固定绑定到 stopFn 触发优雅关闭；
// SIGHUP 仅在业务层未注册处理器时才追加默认行为（记录日志继续运行）。
// sigCh 在锁内赋值后，后续 Register 调用可安全追加新信号到同一 channel。
func (sm *SignalManager) Start(stopFn func()) {
    sm.Lock()
    
    sm.signals[syscall.SIGINT] = []SignalTrap{stopFn}
    sm.signals[syscall.SIGKILL] = []SignalTrap{stopFn}
    sm.signals[syscall.SIGTERM] = []SignalTrap{stopFn}
    
    if _, ok := sm.signals[syscall.SIGHUP]; !ok {
        sm.signals[syscall.SIGHUP] = []SignalTrap{func() {
            slog.Info("received sighup signal")
        }}
    }
    
    allSigs := make([]os.Signal, 0, len(sm.signals))
    for sig := range sm.signals {
        allSigs = append(allSigs, sig)
    }
    sigCh := make(chan os.Signal, 8)
    signal.Notify(sigCh, allSigs...)
    sm.sigCh = sigCh
    
    sm.Unlock()
    
    // 每个信号的处理器并发执行，取读锁后立即释放，避免在 channel 阻塞期间持锁与 Register 的写锁死锁。
    go func() {
        for sig := range sigCh {
            slog.Info("signal received", "signal", sig)
            sm.RLock()
            traps := sm.signals[sig]
            sm.RUnlock()
            var wg sync.WaitGroup
            for _, trap := range traps {
                wg.Go(func() {
                    defer func() {
                        if r := recover(); r != nil {
                            slog.Error("signal handler panicked", "signal", sig, "panic", r, "stack", string(debug.Stack()))
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
func (sm *SignalManager) Stop() {
    sm.Lock()
    defer sm.Unlock()
    if sm.sigCh == nil {
        return
    }
    signal.Stop(sm.sigCh)
    close(sm.sigCh)
    sm.sigCh = nil
}
