package gossip

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/unistack-org/micro/v3/logger"
	"github.com/unistack-org/micro/v3/register"
)

func newMemberlistConfig() *memberlist.Config {
	mc := memberlist.DefaultLANConfig()
	mc.DisableTcpPings = false
	mc.GossipVerifyIncoming = false
	mc.GossipVerifyOutgoing = false
	mc.EnableCompression = false
	mc.PushPullInterval = 3 * time.Second
	mc.LogOutput = os.Stderr
	mc.ProtocolVersion = 4
	mc.Name = uuid.New().String()
	return mc
}

func newRegister(opts ...register.Option) register.Register {
	options := []register.Option{
		ConnectRetry(true),
		ConnectTimeout(60 * time.Second),
	}

	options = append(options, opts...)
	r := NewRegister(options...)
	return r
}

func TestGossipRegisterBroadcast(t *testing.T) {
	if tr := os.Getenv("TRAVIS"); len(tr) > 0 {
		t.Skip()
	}
	ctx := context.Background()

	mc1 := newMemberlistConfig()
	r1 := newRegister(Config(mc1), Address("127.0.0.1:54321"))

	mc2 := newMemberlistConfig()
	r2 := newRegister(Config(mc2), Address("127.0.0.1:54322"), register.Addrs("127.0.0.1:54321"))

	if err := r1.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r2.Init(); err != nil {
		t.Fatal(err)
	}

	if err := r1.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r2.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	defer r1.(*gossipRegister).Stop()
	defer r2.(*gossipRegister).Stop()

	svc1 := &register.Service{Name: "service.1", Version: "0.0.0.1"}
	svc2 := &register.Service{Name: "service.2", Version: "0.0.0.2"}

	if err := r1.Register(ctx, svc1, register.RegisterTTL(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r2.Register(ctx, svc2, register.RegisterTTL(10*time.Second)); err != nil {
		t.Fatal(err)
	}

	var found bool
	svcs, err := r1.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, svc := range svcs {
		if svc.Name == "service.2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("[gossip register] service.2 not found in r1, broadcast not work")
	}

	found = false

	svcs, err = r2.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, svc := range svcs {
		if svc.Name == "service.1" {
			found = true
		}
	}

	if !found {
		t.Fatalf("[gossip register] broadcast failed: service.1 not found in r2")
	}

	if err := r1.Deregister(ctx, svc1); err != nil {
		t.Fatal(err)
	}
	if err := r2.Deregister(ctx, svc2); err != nil {
		t.Fatal(err)
	}

}
func TestGossipRegisterRetry(t *testing.T) {
	if tr := os.Getenv("TRAVIS"); len(tr) > 0 {
		t.Skip()
	}
	ctx := context.Background()
	logger.DefaultLogger = logger.NewLogger(logger.WithLevel(logger.TraceLevel))
	mc1 := newMemberlistConfig()
	r1 := newRegister(Config(mc1), Address("127.0.0.1:54321"))

	mc2 := newMemberlistConfig()
	r2 := newRegister(Config(mc2), Address("127.0.0.1:54322"), register.Addrs("127.0.0.1:54321"))

	if err := r1.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r2.Init(); err != nil {
		t.Fatal(err)
	}

	if err := r1.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r2.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	defer r1.(*gossipRegister).Stop()
	defer r2.(*gossipRegister).Stop()

	svc1 := &register.Service{Name: "service.1", Version: "0.0.0.1"}
	svc2 := &register.Service{Name: "service.2", Version: "0.0.0.2"}

	var mu sync.Mutex
	ch := make(chan struct{})
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				break
			case <-ticker.C:
				mu.Lock()
				if r1 != nil {
					r1.Register(ctx, svc1, register.RegisterTTL(2*time.Second))
				}
				if r2 != nil {
					r2.Register(ctx, svc2, register.RegisterTTL(2*time.Second))
				}
				if ch != nil {
					close(ch)
					ch = nil
				}
				mu.Unlock()
			}
		}
	}()

	<-ch
	var found bool

	svcs, err := r2.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, svc := range svcs {
		if svc.Name == "service.1" {
			found = true
		}
	}

	if !found {
		t.Fatalf("[gossip register] broadcast failed: service.1 not found in r2")
	}

	mu.Lock()
	if err = r1.Disconnect(ctx); err != nil {
		t.Fatalf("[gossip register] failed to stop register: %v", err)
	}
	r1 = nil
	mu.Unlock()

	<-time.After(4 * time.Second)

	found = false
	svcs, err = r2.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, svc := range svcs {
		if svc.Name == "service.1" {
			found = true
		}
	}

	if found {
		t.Fatalf("[gossip register] service.1 found in r2")
	}

	mu.Lock()
	r1 = newRegister(Config(mc1), Address("127.0.0.1:54321"))
	if err := r1.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r1.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Unlock()
	<-time.After(2 * time.Second)

	found = false
	svcs, err = r2.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, svc := range svcs {
		if svc.Name == "service.1" {
			found = true
		}
	}

	if !found {
		t.Fatalf("[gossip register] connect retry failed: service.1 not found in r2")
	}

	if err := r1.Deregister(ctx, svc1); err != nil {
		t.Fatal(err)
	}
	if err := r2.Deregister(ctx, svc2); err != nil {
		t.Fatal(err)
	}

	r1.(*gossipRegister).Stop()
	r2.(*gossipRegister).Stop()
}
