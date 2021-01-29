package gossip

import (
	"github.com/unistack-org/micro/v3/register"
)

type gossipWatcher struct {
	wo   register.WatchOptions
	next chan *register.Result
	stop chan bool
}

func newGossipWatcher(ch chan *register.Result, stop chan bool, opts ...register.WatchOption) (register.Watcher, error) {
	var wo register.WatchOptions
	for _, o := range opts {
		o(&wo)
	}

	return &gossipWatcher{
		wo:   wo,
		next: ch,
		stop: stop,
	}, nil
}

func (m *gossipWatcher) Next() (*register.Result, error) {
	for {
		select {
		case r, ok := <-m.next:
			if !ok {
				return nil, register.ErrWatcherStopped
			}
			// check watch options
			if len(m.wo.Service) > 0 && r.Service.Name != m.wo.Service {
				continue
			}
			nr := &register.Result{}
			*nr = *r
			return nr, nil
		case <-m.stop:
			return nil, register.ErrWatcherStopped
		}
	}
}

func (m *gossipWatcher) Stop() {
	select {
	case <-m.stop:
		return
	default:
		close(m.stop)
	}
}
