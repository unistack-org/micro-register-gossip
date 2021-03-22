// Package gossip provides a gossip register based on hashicorp/memberlist
package gossip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/mitchellh/hashstructure/v2"
	pb "github.com/unistack-org/micro-register-gossip/v3/proto"
	"github.com/unistack-org/micro/v3/logger"
	"github.com/unistack-org/micro/v3/register"
	regutil "github.com/unistack-org/micro/v3/util/register"
	"google.golang.org/protobuf/proto"
)

// use register.Result int32 values after it switches from string to int32 types
// type actionType int32
// type updateType int32

var (
	ErrNotConnected = errors.New("register not connected")
)

const (
	actionTypeInvalid int32 = iota
	actionTypeCreate
	actionTypeDelete
	actionTypeUpdate
	actionTypeSync
)

const (
	nodeActionUnknown int32 = iota
	nodeActionJoin
	nodeActionLeave
	nodeActionUpdate
)

func actionTypeString(t int32) string {
	switch t {
	case actionTypeCreate:
		return "create"
	case actionTypeDelete:
		return "delete"
	case actionTypeUpdate:
		return "update"
	case actionTypeSync:
		return "sync"
	}
	return "invalid"
}

const (
	updateTypeInvalid int32 = iota
	updateTypeService
)

type broadcast struct {
	opts   register.Options
	update *pb.Update
	notify chan<- struct{}
}

type delegate struct {
	queue   *memberlist.TransmitLimitedQueue
	updates chan *update
}

type event struct {
	action int32
	node   string
}

type eventDelegate struct {
	events chan *event
}

func (ed *eventDelegate) NotifyJoin(n *memberlist.Node) {
	ed.events <- &event{action: nodeActionJoin, node: n.Address()}
}
func (ed *eventDelegate) NotifyLeave(n *memberlist.Node) {
	ed.events <- &event{action: nodeActionLeave, node: n.Address()}
}
func (ed *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	ed.events <- &event{action: nodeActionUpdate, node: n.Address()}
}

type gossipRegister struct {
	queue       *memberlist.TransmitLimitedQueue
	updates     chan *update
	events      chan *event
	opts        register.Options
	member      *memberlist.Memberlist
	interval    time.Duration
	tcpInterval time.Duration

	connected      bool
	connectRetry   bool
	connectTimeout time.Duration
	sync.RWMutex
	services map[string][]*register.Service

	watchers map[string]chan *register.Result

	mtu     int
	addrs   []string
	members map[string]int32
	done    chan bool
}

type update struct {
	Update  *pb.Update
	Service *register.Service
	sync    chan *register.Service
}

type updates struct {
	sync.RWMutex
	services map[uint64]*update
}

var (
	// You should change this if using secure
	DefaultSecret = []byte("micro-gossip-key") // exactly 16 bytes
	ExpiryTick    = time.Second * 1            // needs to be smaller than register.RegisterTTL
	MaxPacketSize = 512
)

func configure(g *gossipRegister, opts ...register.Option) error {
	// loop through address list and get valid entries
	addrs := func(curAddrs []string) []string {
		var newAddrs []string
		for _, addr := range curAddrs {
			if trimAddr := strings.TrimSpace(addr); len(trimAddr) > 0 {
				newAddrs = append(newAddrs, trimAddr)
			}
		}
		return newAddrs
	}

	// current address list
	curAddrs := addrs(g.opts.Addrs)

	for _, o := range opts {
		o(&g.opts)
	}

	// new address list
	newAddrs := addrs(g.opts.Addrs)

	// no new nodes and existing member. no configure
	if (len(newAddrs) == len(curAddrs)) && g.member != nil {
		return nil
	}

	// shutdown old member
	g.Stop()

	// lock internals
	g.Lock()

	// new done chan
	g.done = make(chan bool)

	// replace addresses
	curAddrs = newAddrs

	// create a new default config
	c := memberlist.DefaultLocalConfig()

	// sane good default options
	c.LogOutput = ioutil.Discard // log to /dev/null
	c.PushPullInterval = 0       // disable expensive tcp push/pull
	c.ProtocolVersion = 4        // suport latest stable features

	// set config from options
	if config, ok := g.opts.Context.Value(configKey{}).(*memberlist.Config); ok && config != nil {
		c = config
	}

	// set address
	if address, ok := g.opts.Context.Value(addressKey{}).(string); ok {
		host, port, err := net.SplitHostPort(address)
		if err == nil {
			p, err := strconv.Atoi(port)
			if err == nil {
				c.BindPort = p
			}
			c.BindAddr = host
		}
	} else {
		// set bind to random port
		c.BindPort = 0
	}

	// set the advertise address
	if advertise, ok := g.opts.Context.Value(advertiseKey{}).(string); ok {
		host, port, err := net.SplitHostPort(advertise)
		if err == nil {
			p, err := strconv.Atoi(port)
			if err == nil {
				c.AdvertisePort = p
			}
			c.AdvertiseAddr = host
		}
	}

	// machine hostname
	hostname, _ := os.Hostname()

	// set the name
	c.Name = strings.Join([]string{"micro", hostname, uuid.New().String()}, "-")

	// set a secret key if secure
	k, ok := g.opts.Context.Value(secretKey{}).([]byte)
	if !ok {
		// use the default secret
		k = DefaultSecret
	}
	c.SecretKey = k

	// set connect retry
	if v, ok := g.opts.Context.Value(connectRetryKey{}).(bool); ok && v {
		g.connectRetry = true
	}

	// set connect timeout
	if td, ok := g.opts.Context.Value(connectTimeoutKey{}).(time.Duration); ok {
		g.connectTimeout = td
	}

	// create a queue
	queue := &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return len(curAddrs)
		},
		RetransmitMult: 3,
	}

	// set the delegate
	c.Delegate = &delegate{
		updates: g.updates,
		queue:   queue,
	}

	if g.connectRetry {
		c.Events = &eventDelegate{
			events: g.events,
		}
	}
	// create the memberlist
	m, err := memberlist.Create(c)
	if err != nil {
		return err
	}

	if len(curAddrs) > 0 {
		for _, addr := range curAddrs {
			g.members[addr] = nodeActionUnknown
		}
	}

	g.tcpInterval = c.PushPullInterval
	g.addrs = curAddrs
	g.queue = queue
	g.member = m
	g.interval = c.GossipInterval

	g.Unlock()

	if g.opts.Logger.V(logger.InfoLevel) {
		g.opts.Logger.Infof(g.opts.Context, "[gossip] Register Listening on %s", m.LocalNode().Address())
	}

	g.addrs = curAddrs
	// try connect
	return g.connect(curAddrs)
}

func (*broadcast) UniqueBroadcast() {}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	up, err := proto.Marshal(b.update)
	if err != nil {
		return nil
	}
	if l := len(up); l > MaxPacketSize && b.opts.Logger.V(logger.InfoLevel) {
		b.opts.Logger.Infof(b.opts.Context, "[gossip] Register broadcast message size %d bigger then MaxPacketSize %d", l, MaxPacketSize)
	}
	return up
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}

	go func() {
		up := new(pb.Update)
		if err := proto.Unmarshal(b, up); err != nil {
			return
		}

		// only process service action
		if up.Type != updateTypeService {
			return
		}

		var service *register.Service

		switch up.Metadata["Content-Type"] {
		case "application/json":
			if err := json.Unmarshal(up.Data, &service); err != nil {
				return
			}
		// no other content type
		default:
			return
		}

		// send update
		d.updates <- &update{
			Update:  up,
			Service: service,
		}
	}()
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.queue.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	if !join {
		return []byte{}
	}

	syncCh := make(chan *register.Service, 1)
	services := map[string][]*register.Service{}

	d.updates <- &update{
		Update: &pb.Update{
			Action: actionTypeSync,
		},
		sync: syncCh,
	}

	for srv := range syncCh {
		services[srv.Name] = append(services[srv.Name], srv)
	}

	b, _ := json.Marshal(services)
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	if !join {
		return
	}

	var services map[string][]*register.Service
	if err := json.Unmarshal(buf, &services); err != nil {
		return
	}
	for _, service := range services {
		for _, srv := range service {
			d.updates <- &update{
				Update:  &pb.Update{Action: actionTypeCreate},
				Service: srv,
				sync:    nil,
			}
		}
	}
}

func (g *gossipRegister) Connect(ctx context.Context) error {
	g.connected = true
	return nil
	//return g.connect(g.opts.Addrs)
}

func (g *gossipRegister) Disconnect(ctx context.Context) error {
	return g.Stop()
}

func (g *gossipRegister) connect(addrs []string) error {
	if len(addrs) == 0 {
		return nil
	}

	timeout := make(<-chan time.Time)

	if g.connectTimeout > 0 {
		timeout = time.After(g.connectTimeout)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fn := func() (int, error) {
		return g.member.Join(addrs)
	}

	// don't wait for first try
	if _, err := fn(); err == nil {
		return nil
	}

	// wait loop
	for {
		select {
		// context closed
		case <-g.opts.Context.Done():
			return nil
		// call close, don't wait anymore
		case <-g.done:
			return nil
		//  in case of timeout fail with a timeout error
		case <-timeout:
			return fmt.Errorf("[gossip] Register connect timeout %v", g.addrs)
		// got a tick, try to connect
		case <-ticker.C:
			if _, err := fn(); err == nil {
				if g.opts.Logger.V(logger.DebugLevel) {
					g.opts.Logger.Debugf(g.opts.Context, "[gossip] Register connect success for %v", g.addrs)
				}
				return nil
			} else {
				if g.opts.Logger.V(logger.DebugLevel) {
					g.opts.Logger.Debugf(g.opts.Context, "[gossip] Register connect failed for %v", g.addrs)
				}
			}
		}
	}

	return nil
}

func (g *gossipRegister) publish(action string, services []*register.Service) {
	g.RLock()
	for _, sub := range g.watchers {
		go func(sub chan *register.Result) {
			for _, service := range services {
				sub <- &register.Result{Action: action, Service: service}
			}
		}(sub)
	}
	g.RUnlock()
}

func (g *gossipRegister) subscribe() (chan *register.Result, chan bool) {
	next := make(chan *register.Result, 10)
	exit := make(chan bool)

	id := uuid.New().String()

	g.Lock()
	g.watchers[id] = next
	g.Unlock()

	go func() {
		<-exit
		g.Lock()
		delete(g.watchers, id)
		close(next)
		g.Unlock()
	}()

	return next, exit
}

func (g *gossipRegister) Stop() error {
	g.connected = false
	select {
	case <-g.done:
		return nil
	default:
		close(g.done)
		g.Lock()
		if g.member != nil {
			g.member.Leave(g.interval * 2)
			g.member.Shutdown()
			g.member = nil
		}
		g.Unlock()
	}
	return nil
}

// connectLoop attempts to reconnect to the memberlist
func (g *gossipRegister) connectLoop() {
	// try every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-g.done:
			return
		case <-g.opts.Context.Done():
			g.Stop()
			return
		case <-ticker.C:
			var addrs []string

			g.RLock()

			// only process if we have a memberlist
			if g.member == nil {
				g.RUnlock()
				continue
			}

			// self
			local := g.member.LocalNode().Address()

			// operate on each member
			for node, action := range g.members {
				switch action {
				// process leave event
				case nodeActionLeave:
					// don't process self
					if node == local {
						continue
					}
					addrs = append(addrs, node)
				}
			}

			g.RUnlock()

			// connect to all the members
			// TODO: only connect to new members
			if len(addrs) > 0 {
				g.connect(addrs)
			}
		}
	}
}

func (g *gossipRegister) expiryLoop(updates *updates) {
	ticker := time.NewTicker(ExpiryTick)
	defer ticker.Stop()

	g.RLock()
	done := g.done
	g.RUnlock()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			now := uint64(time.Now().UnixNano())

			updates.Lock()

			// process all the updates
			for k, v := range updates.services {
				// check if expiry time has passed
				if d := (v.Update.Expires); d < now {
					// delete from records
					delete(updates.services, k)
					// set to delete
					v.Update.Action = actionTypeDelete
					// fire a new update
					g.updates <- v
				}
			}

			updates.Unlock()
		}
	}
}

// process member events
func (g *gossipRegister) eventLoop() {
	g.RLock()
	done := g.done
	g.RUnlock()
	for {
		select {
		// return when done
		case <-done:
			return
		case ev := <-g.events:
			// TODO: nonblocking update
			g.Lock()
			if _, ok := g.members[ev.node]; ok {
				g.members[ev.node] = ev.action
			}
			g.Unlock()
		}
	}
}

func (g *gossipRegister) run() {
	updates := &updates{
		services: make(map[uint64]*update),
	}

	// expiry loop
	go g.expiryLoop(updates)

	// event loop
	go g.eventLoop()

	g.RLock()
	// connect loop
	if g.connectRetry {
		go g.connectLoop()
	}
	g.RUnlock()

	// process the updates
	for u := range g.updates {
		switch u.Update.Action {
		case actionTypeCreate:
			g.Lock()
			if service, ok := g.services[u.Service.Name]; !ok {
				g.services[u.Service.Name] = []*register.Service{u.Service}

			} else {
				g.services[u.Service.Name] = regutil.Merge(service, []*register.Service{u.Service})
			}
			g.Unlock()

			// publish update to watchers
			go g.publish(actionTypeString(actionTypeCreate), []*register.Service{u.Service})

			// we need to expire the node at some point in the future
			if u.Update.Expires > 0 {
				// create a hash of this service
				if hash, err := hashstructure.Hash(u.Service, hashstructure.FormatV2, nil); err == nil {
					updates.Lock()
					updates.services[hash] = u
					updates.Unlock()
				}
			}
		case actionTypeDelete:
			g.Lock()
			if service, ok := g.services[u.Service.Name]; ok {
				if services := regutil.Remove(service, []*register.Service{u.Service}); len(services) == 0 {
					delete(g.services, u.Service.Name)
				} else {
					g.services[u.Service.Name] = services
				}
			}
			g.Unlock()

			// publish update to watchers
			go g.publish(actionTypeString(actionTypeDelete), []*register.Service{u.Service})

			// delete from expiry checks
			if hash, err := hashstructure.Hash(u.Service, hashstructure.FormatV2, nil); err == nil {
				updates.Lock()
				delete(updates.services, hash)
				updates.Unlock()
			}
		case actionTypeSync:
			// no sync channel provided
			if u.sync == nil {
				continue
			}

			g.RLock()

			// push all services through the sync chan
			for _, service := range g.services {
				for _, srv := range service {
					u.sync <- srv
				}

				// publish to watchers
				go g.publish(actionTypeString(actionTypeCreate), service)
			}

			g.RUnlock()

			// close the sync chan
			close(u.sync)
		}
	}
}

func (g *gossipRegister) Init(opts ...register.Option) error {
	go g.run()

	// configure the gossiper
	if err := configure(g, opts...); err != nil {
		return err
	}
	// wait for setup
	<-time.After(g.interval * 2)
	return nil
}

func (g *gossipRegister) Options() register.Options {
	return g.opts
}

func (g *gossipRegister) Name() string {
	return g.opts.Name
}

func (g *gossipRegister) Register(ctx context.Context, s *register.Service, opts ...register.RegisterOption) error {
	if !g.connected {
		return ErrNotConnected
	}
	if g.opts.Logger.V(logger.DebugLevel) {
		g.opts.Logger.Debugf(g.opts.Context, "[gossip] Register registering service: %s", s.Name)
	}

	b, err := json.Marshal(s)
	if err != nil {
		return err
	}

	g.Lock()
	if service, ok := g.services[s.Name]; !ok {
		g.services[s.Name] = []*register.Service{s}
	} else {
		g.services[s.Name] = regutil.Merge(service, []*register.Service{s})
	}
	g.Unlock()

	options := register.NewRegisterOptions(opts...)

	if options.TTL == 0 && g.tcpInterval == 0 {
		return fmt.Errorf("[gossip] Require register TTL or interval for memberlist.Config")
	}

	up := &pb.Update{
		Expires: uint64(time.Now().Add(options.TTL).UnixNano()),
		Action:  actionTypeCreate,
		Type:    updateTypeService,
		Metadata: map[string]string{
			"Content-Type": "application/json",
		},
		Data: b,
	}

	g.queue.QueueBroadcast(&broadcast{
		opts:   g.opts,
		update: up,
		notify: nil,
	})

	// send update to local watchers
	g.updates <- &update{
		Update:  up,
		Service: s,
	}

	// wait
	<-time.After(g.interval * 2)

	return nil
}

func (g *gossipRegister) Deregister(ctx context.Context, s *register.Service, opts ...register.DeregisterOption) error {
	if !g.connected {
		return ErrNotConnected
	}

	if g.opts.Logger.V(logger.DebugLevel) {
		g.opts.Logger.Debugf(g.opts.Context, "[gossip] Register deregistering service: %s", s.Name)
	}

	b, err := json.Marshal(s)
	if err != nil {
		return err
	}

	g.Lock()
	if service, ok := g.services[s.Name]; ok {
		if services := regutil.Remove(service, []*register.Service{s}); len(services) == 0 {
			delete(g.services, s.Name)
		} else {
			g.services[s.Name] = services
		}
	}
	g.Unlock()

	up := &pb.Update{
		Action: actionTypeDelete,
		Type:   updateTypeService,
		Metadata: map[string]string{
			"Content-Type": "application/json",
		},
		Data: b,
	}

	g.queue.QueueBroadcast(&broadcast{
		update: up,
		notify: nil,
	})

	// send update to local watchers
	g.updates <- &update{
		Update:  up,
		Service: s,
	}

	// wait
	<-time.After(g.interval * 2)

	return nil
}

func (g *gossipRegister) LookupService(ctx context.Context, name string, opts ...register.LookupOption) ([]*register.Service, error) {
	g.RLock()
	service, ok := g.services[name]
	g.RUnlock()
	if !ok {
		return nil, register.ErrNotFound
	}
	return service, nil
}

func (g *gossipRegister) ListServices(ctx context.Context, opts ...register.ListOption) ([]*register.Service, error) {
	g.RLock()
	services := make([]*register.Service, 0, len(g.services))
	for _, service := range g.services {
		services = append(services, service...)
	}
	g.RUnlock()
	return services, nil
}

func (g *gossipRegister) Watch(ctx context.Context, opts ...register.WatchOption) (register.Watcher, error) {
	n, e := g.subscribe()
	return newGossipWatcher(n, e, opts...)
}

func (g *gossipRegister) String() string {
	return "gossip"
}

func NewRegister(opts ...register.Option) register.Register {
	g := &gossipRegister{
		opts:     register.NewOptions(opts...),
		done:     make(chan bool),
		events:   make(chan *event, 100),
		updates:  make(chan *update, 100),
		services: make(map[string][]*register.Service),
		watchers: make(map[string]chan *register.Result),
		members:  make(map[string]int32),
	}
	return g
}
