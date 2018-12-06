// Package Gossip provides a gossip registry based on hashicorp/memberlist
package gossip

import (
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/registry/gossip"
)

// NewRegistry returns a new gossip registry
func NewRegistry(opts ...registry.Option) registry.Registry {
	return gossip.NewRegistry(opts...)
}
