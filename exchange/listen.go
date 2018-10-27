package exchange

import (
	"github.com/niwho/distribute_cache/consistent"
	"github.com/perlin-network/noise/crypto/ed25519"
	"github.com/perlin-network/noise/log"
	"github.com/perlin-network/noise/network"
	"github.com/perlin-network/noise/network/backoff"
	"github.com/perlin-network/noise/network/discovery"
)

type Config struct {
	Protocol       string
	Host           string
	Port           uint16
	BootstrapPeers []string
	Handle         HANDLE
	Con            consistent.ICONSISTENT
}

func Listen(cfg Config) error {
	if cfg.Con == nil {
		cfg.Con = consistent.NewDistrubuteConsistent()
	}
	keys := ed25519.RandomKeyPair()
	log.Info().Msgf("Private Key: %s", keys.PrivateKeyHex())
	log.Info().Msgf("Public Key: %s", keys.PublicKeyHex())

	builder := network.NewBuilder()
	builder.SetKeys(keys)
	builder.SetAddress(network.FormatAddress(cfg.Protocol, cfg.Host, cfg.Port))

	// Register peer discovery plugin.
	builder.AddPlugin(new(discovery.Plugin))
	builder.AddPlugin(new(backoff.Plugin))

	// Add custom chat plugin.
	builder.AddPlugin(ExchangeData{handle: cfg.Handle, con: cfg.Con})

	net, err := builder.Build()
	if err != nil {
		log.Fatal().Err(err)
		return err
	}

	go net.Listen()

	if len(cfg.BootstrapPeers) > 0 {
		net.Bootstrap(cfg.BootstrapPeers...)
	}
	return nil
}
