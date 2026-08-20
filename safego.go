package xhive

import (
	"context"
	"log/slog"
	"runtime/debug"
)

// SafeGo 在独立 goroutine 中执行 fn，并自动恢复其内部的 panic 以避免整个进程崩溃。
func SafeGo(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("skeleton goroutine panic recovered",
					slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			}
		}()
		fn()
	}()
}

// SafeGoContext 语义同 SafeGo，额外在启动时检查 ctx 是否已被取消：
// 若调用时 ctx 已处于取消状态，则跳过执行；否则执行期间的取消需 fn 自行通过 ctx.Done() 感知，
// SafeGoContext 不会在执行过程中主动中断 fn。
func SafeGoContext(ctx context.Context, fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("skeleton goroutine panic recovered",
					slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			}
		}()

		// 如果 context 已经被取消，直接退出，不执行 fn。
		select {
		case <-ctx.Done():
			slog.WarnContext(ctx, "goroutine skipped: context already canceled",
				slog.Any("error", ctx.Err()))
			return
		default:
			fn(ctx)
		}
	}()
}
