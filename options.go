package gossip

import (
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/unistack-org/micro/v3/register"
)

type secretKey struct{}
type addressKey struct{}
type configKey struct{}
type advertiseKey struct{}
type connectTimeoutKey struct{}
type connectRetryKey struct{}

// Secret specifies an encryption key. The value should be either
// 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.
func Secret(k []byte) register.Option {
	return register.SetOption(secretKey{}, k)
}

// Address to bind to - host:port
func Address(a string) register.Option {
	return register.SetOption(addressKey{}, a)
}

// Config sets *memberlist.Config for configuring gossip
func Config(c *memberlist.Config) register.Option {
	return register.SetOption(configKey{}, c)
}

// The address to advertise for other gossip members to connect to - host:port
func Advertise(a string) register.Option {
	return register.SetOption(advertiseKey{}, a)
}

// ConnectTimeout sets the register connect timeout. Use -1 to specify infinite timeout
func ConnectTimeout(td time.Duration) register.Option {
	return register.SetOption(connectTimeoutKey{}, td)
}

// ConnectRetry enables reconnect to register then connection closed,
// use with ConnectTimeout to specify how long retry
func ConnectRetry(v bool) register.Option {
	return register.SetOption(connectRetryKey{}, v)
}
