package xhive

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestAppRegisterNilStatsNoRPCAndStopIdempotent(t *testing.T) {
	a := newApp()
	noRPC := newTestModule("no-rpc")
	noRPC.server = nil
	if err := a.Register(nil, noRPC); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if len(a.modules) != 1 {
		t.Fatalf("registered module count = %d, want 1", len(a.modules))
	}
	if got := a.ChanRPC("no-rpc"); got != nil {
		t.Fatalf("ChanRPC for no-rpc = %#v, want nil", got)
	}
	stats := a.Stats()
	if !strings.Contains(stats, "static: no-rpc, rpc_queue_length: N/A") {
		t.Fatalf("Stats missing N/A rpc length: %s", stats)
	}

	a.stop()
	if a.State() != AppStateNone {
		t.Fatalf("stop before start state = %d, want none", a.State())
	}
	if !a.start() {
		// 空启动应失败且仍保持 None，随后再次 stop 应保持幂等且不 panic。
		if a.State() != AppStateNone {
			t.Fatalf("empty start state = %d, want none", a.State())
		}
		a.stop()
	}
}

func TestAppStartCannotStartTwiceAndStopOrder(t *testing.T) {
	a := newApp()
	order := make(chan string, 4)
	m1 := newTestModule("first")
	m2 := newTestModule("second")
	m1.destroyHook = func() { order <- "first" }
	m2.destroyHook = func() { order <- "second" }

	if !a.start(m1, m2) {
		t.Fatal("start should succeed")
	}
	waitClosed(t, m1.runStarted, time.Second)
	waitClosed(t, m2.runStarted, time.Second)
	if a.start(newTestModule("again")) {
		t.Fatal("start twice should fail")
	}

	a.stop()
	want := []string{"second", "first"}
	got := []string{<-order, <-order}
	if !slices.Equal(got, want) {
		t.Fatalf("destroy order = %v, want %v", got, want)
	}
	a.setState(AppStateStop)
	a.stop()
	if a.State() != AppStateStop {
		t.Fatalf("stop while stopping state = %d, want stop", a.State())
	}
}

func TestAppDynamicPartialFailureKeepsStartedModulesAndRemoveAll(t *testing.T) {
	a := newApp()
	good := newTestModule("good-dyn")
	bad := newTestModule("bad-dyn")
	bad.initErr = errors.New("bad init")

	if err := a.AddDynamicModules(good, bad); err == nil {
		t.Fatal("AddDynamicModules should fail when later module init fails")
	}
	waitClosed(t, good.runStarted, time.Second)
	if a.ChanRPC("good-dyn") != good.server {
		t.Fatal("successfully started dynamic module should remain stored")
	}
	if a.ChanRPC("bad-dyn") != nil {
		t.Fatal("failed dynamic module should not be stored")
	}

	a.removeAllDynamicModules()
	waitClosed(t, good.runStopped, time.Second)
	if len(a.DynamicModules()) != 0 {
		t.Fatalf("dynamic modules after removeAll = %v, want empty", a.DynamicModules())
	}

	a.dynamicModules.Store("broken", "not-wrapper")
	if a.RemoveDynamicModule("broken") {
		t.Fatal("RemoveDynamicModule should return false for non-wrapper value")
	}
	a.dynamicModules.Delete("broken")
}
