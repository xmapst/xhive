package xhive

import (
	"syscall"
	"testing"
	"time"
)

func TestSignalManagerStopBeforeStartDefaultSIGHUPAndPanicRecovery(t *testing.T) {
	sm := NewSignalManager()
	sm.Stop()

	first := make(chan struct{}, 1)
	second := make(chan struct{}, 1)
	if err := sm.Register(func() { first <- struct{}{} }, syscall.SIGHUP); err != nil {
		t.Fatalf("Register first trap failed: %v", err)
	}
	if err := sm.Register(func() { panic("signal panic") }, syscall.SIGHUP); err != nil {
		t.Fatalf("Register panic trap failed: %v", err)
	}
	if err := sm.Register(func() { second <- struct{}{} }, syscall.SIGHUP); err != nil {
		t.Fatalf("Register second trap failed: %v", err)
	}
	sm.Start(func() {})
	sm.sigCh <- syscall.SIGHUP
	waitClosed(t, first, time.Second)
	waitClosed(t, second, time.Second)
	sm.Stop()
	sm.Stop()
	if sm.sigCh != nil {
		t.Fatal("sigCh should be nil after Stop")
	}

	defaultSM := NewSignalManager()
	defaultSM.Start(func() {})
	defer defaultSM.Stop()
	if traps := defaultSM.signals[syscall.SIGHUP]; len(traps) != 1 {
		t.Fatalf("default SIGHUP trap count = %d, want 1", len(traps))
	}
}
