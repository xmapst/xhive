package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

type testModule struct {
	name         string
	server       *chanrpc.Server
	ready        chan struct{}
	readyOnce    sync.Once
	initFn       func() error
	initCalls    atomic.Int32
	serveCalls   atomic.Int32
	destroyCalls atomic.Int32
	closeCalls   atomic.Int32
}

func newTestModule(name string) *testModule {
	return &testModule{name: name, ready: make(chan struct{})}
}

func (m *testModule) Name() string             { return m.name }
func (m *testModule) Priority() uint           { return 0 }
func (m *testModule) Ready() <-chan struct{}   { return m.ready }
func (m *testModule) ChanRPC() *chanrpc.Server { return m.server }

func (m *testModule) OnInit() error {
	m.initCalls.Add(1)
	if m.initFn != nil {
		return m.initFn()
	}
	return nil
}

func (m *testModule) Serve(ctx context.Context) {
	m.serveCalls.Add(1)
	m.readyOnce.Do(func() { close(m.ready) })
	<-ctx.Done()
}

func (m *testModule) OnDestroy() { m.destroyCalls.Add(1) }
func (m *testModule) Close() error {
	m.closeCalls.Add(1)
	return nil
}

func newTestApp(t *testing.T) *App {
	t.Helper()
	a := New(WithShutdownTimeout(time.Second))
	t.Cleanup(func() {
		a.stop()
		// Dynamic modules can be loaded before the static application starts.
		a.removeAllDynamicModules()
		for _, wrapper := range a.modules {
			wrapper.cancel()
		}
	})
	return a
}

func assertModuleCalls(t *testing.T, m *testModule, want [4]int32) {
	t.Helper()
	got := [4]int32{m.initCalls.Load(), m.serveCalls.Load(), m.destroyCalls.Load(), m.closeCalls.Load()}
	if got != want {
		t.Errorf("module %q calls (init, serve, destroy, close) = %v, want %v", m.name, got, want)
	}
}

func assertDuplicateName(t *testing.T, err error, name string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), name) {
		t.Errorf("duplicate module %q error = %v", name, err)
	}
}

func TestRegisterRejectsDuplicateNames(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, sameInstance := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%t/same_instance=%t", existing, sameInstance), func(t *testing.T) {
				a := newTestApp(t)
				first := newTestModule("worker")
				duplicate := newTestModule("worker")
				if sameInstance {
					duplicate = first
				}
				other := newTestModule("other")
				mods := []IModule{other, first, duplicate}
				wantCount := 0
				if existing {
					if err := a.Register(first); err != nil {
						t.Fatal(err)
					}
					mods = []IModule{other, duplicate}
					wantCount = 1
				}

				assertDuplicateName(t, a.Register(mods...), "worker")
				if len(a.modules) != wantCount {
					t.Errorf("failed batch changed registered modules: got %d, want %d", len(a.modules), wantCount)
				}
				assertModuleCalls(t, first, [4]int32{})
				assertModuleCalls(t, duplicate, [4]int32{})
				assertModuleCalls(t, other, [4]int32{})
				if err := a.Register(other); err != nil {
					t.Errorf("failed batch reserved an unrelated name: %v", err)
				}
			})
		}
	}
}

func TestStartRejectsDuplicateNamesBeforeInitialization(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, sameInstance := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%t/same_instance=%t", existing, sameInstance), func(t *testing.T) {
				a := newTestApp(t)
				first := newTestModule("worker")
				duplicate := newTestModule("worker")
				if sameInstance {
					duplicate = first
				}
				mods := []IModule{first, duplicate}
				wantCount := 0
				if existing {
					if err := a.Register(first); err != nil {
						t.Fatal(err)
					}
					mods = []IModule{duplicate}
					wantCount = 1
				}

				assertDuplicateName(t, a.start(mods...), "worker")
				if len(a.modules) != wantCount {
					t.Errorf("failed start changed registered modules: got %d, want %d", len(a.modules), wantCount)
				}
				assertModuleCalls(t, first, [4]int32{})
				assertModuleCalls(t, duplicate, [4]int32{})
				if state := a.State(); state != StateNone {
					t.Errorf("state = %d, want StateNone", state)
				}
			})
		}
	}
}

func TestRunReturnsDuplicateNameError(t *testing.T) {
	a := newTestApp(t)
	first := newTestModule("worker")
	// Ensure a regression returns from Run even if OnInit is mistakenly called.
	first.initFn = func() error { return errors.New("unexpected initialization") }
	duplicate := newTestModule("worker")
	if err := a.Register(first); err != nil {
		t.Fatal(err)
	}
	err := a.Run(duplicate)
	assertDuplicateName(t, err, "worker")
	assertModuleCalls(t, first, [4]int32{})
	assertModuleCalls(t, duplicate, [4]int32{})
}

func TestAddDynamicModulesRejectsStaticName(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprintf("running=%t", running), func(t *testing.T) {
			a := newTestApp(t)
			static := newTestModule("worker")
			if err := a.Register(static); err != nil {
				t.Fatal(err)
			}
			if running {
				if err := a.start(); err != nil {
					t.Fatal(err)
				}
			}
			duplicate := newTestModule("worker")
			other := newTestModule("other")
			results, err := a.AddDynamicModules(duplicate, other)
			assertDuplicateName(t, err, "worker")
			if len(results) != 2 || results[0].Err == nil || results[1].Err != nil {
				t.Fatalf("dynamic results = %+v, want duplicate failure followed by success", results)
			}
			assertModuleCalls(t, duplicate, [4]int32{})
			assertModuleCalls(t, other, [4]int32{1, 1, 0, 0})
			if names := a.DynamicModules(); len(names) != 1 || names[0] != "other" {
				t.Errorf("dynamic modules = %v, want [other]", names)
			}
		})
	}
}

func TestStaticRegistrationRejectsInitializingDynamicName(t *testing.T) {
	for _, start := range []bool{false, true} {
		t.Run(fmt.Sprintf("start=%t", start), func(t *testing.T) {
			a := newTestApp(t)
			dynamic := newTestModule("worker")
			entered := make(chan struct{})
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			dynamic.initFn = func() error {
				close(entered)
				<-release
				return nil
			}
			done := make(chan error, 1)
			go func() {
				_, err := a.AddDynamicModules(dynamic)
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("dynamic OnInit did not start")
			}
			duplicate := newTestModule("worker")
			other := newTestModule("other")
			var err error
			if start {
				err = a.start(other, duplicate)
			} else {
				err = a.Register(other, duplicate)
			}
			assertDuplicateName(t, err, "worker")
			assertModuleCalls(t, duplicate, [4]int32{})
			assertModuleCalls(t, other, [4]int32{})
			if len(a.modules) != 0 {
				t.Errorf("failed registration left %d static modules", len(a.modules))
			}
			unblock()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("dynamic registration did not finish")
			}
			assertModuleCalls(t, dynamic, [4]int32{1, 1, 0, 0})
		})
	}
}

func TestConcurrentStaticAndDynamicRegistrationReservesName(t *testing.T) {
	for i := range 16 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			a := newTestApp(t)
			static := newTestModule("worker")
			dynamic := newTestModule("worker")
			gate := make(chan struct{})
			staticResult := make(chan error, 1)
			dynamicResult := make(chan error, 1)
			go func() {
				<-gate
				staticResult <- a.Register(static)
			}()
			go func() {
				<-gate
				_, err := a.AddDynamicModules(dynamic)
				dynamicResult <- err
			}()
			close(gate)
			staticErr, dynamicErr := <-staticResult, <-dynamicResult
			if (staticErr == nil) == (dynamicErr == nil) {
				t.Errorf("exactly one registration must succeed: static=%v, dynamic=%v", staticErr, dynamicErr)
			}
		})
	}
}

func TestDynamicDuplicateKeepsOriginalRPC(t *testing.T) {
	for _, sameInstance := range []bool{false, true} {
		t.Run(fmt.Sprintf("same_instance=%t", sameInstance), func(t *testing.T) {
			a := newTestApp(t)
			original := newTestModule("worker")
			original.server = chanrpc.NewServer()
			if _, err := a.AddDynamicModules(original); err != nil {
				t.Fatal(err)
			}
			duplicate := newTestModule("worker")
			if sameInstance {
				duplicate = original
			}
			results, err := a.AddDynamicModules(duplicate)
			assertDuplicateName(t, err, "worker")
			if len(results) != 1 || results[0].Err == nil {
				t.Fatalf("duplicate results = %+v", results)
			}
			if got := a.ChanRPC("worker"); got != original.server {
				t.Errorf("ChanRPC(worker) = %p, want original server %p", got, original.server)
			}
			assertModuleCalls(t, original, [4]int32{1, 1, 0, 0})
			if !sameInstance {
				assertModuleCalls(t, duplicate, [4]int32{})
			}
		})
	}
}

func TestFailedDynamicInitializationReleasesName(t *testing.T) {
	a := newTestApp(t)
	failed := newTestModule("worker")
	initErr := errors.New("initialization failed")
	failed.initFn = func() error { return initErr }
	results, err := a.AddDynamicModules(failed)
	if err == nil || len(results) != 1 || !errors.Is(results[0].Err, initErr) {
		t.Fatalf("dynamic results = %+v, err = %v", results, err)
	}
	replacement := newTestModule("worker")
	if err := a.Register(replacement); err != nil {
		t.Fatal(err)
	}
	if err := a.start(); err != nil {
		t.Fatal(err)
	}
	assertModuleCalls(t, failed, [4]int32{1, 0, 0, 0})
	assertModuleCalls(t, replacement, [4]int32{1, 1, 0, 0})
}

func TestModuleNamesAreScopedToApp(t *testing.T) {
	var modules []*testModule
	var apps []*App
	for range 2 {
		a := newTestApp(t)
		mod := newTestModule("worker")
		mod.server = chanrpc.NewServer()
		if err := a.Register(nil, mod); err != nil {
			t.Fatal(err)
		}
		if err := a.start(nil, newTestModule("other")); err != nil {
			t.Fatal(err)
		}
		if got := a.ChanRPC("worker"); got != mod.server {
			t.Errorf("ChanRPC(worker) = %p, want %p", got, mod.server)
		}
		assertModuleCalls(t, mod, [4]int32{1, 1, 0, 0})
		apps = append(apps, a)
		modules = append(modules, mod)
	}
	for i, a := range apps {
		a.stop()
		assertModuleCalls(t, modules[i], [4]int32{1, 1, 1, 1})
	}
}
