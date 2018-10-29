package exchange

import (
	"context"
	"errors"

	"fmt"
	"github.com/niwho/distribute_cache/broadcast_node"
	"github.com/niwho/distribute_cache/consistent"
	"github.com/niwho/distribute_cache/proto"
	"github.com/perlin-network/noise/crypto/ed25519"
	"github.com/perlin-network/noise/log"
	"github.com/perlin-network/noise/network"
	"github.com/perlin-network/noise/network/backoff"
	"github.com/perlin-network/noise/network/discovery"
	"time"
)

var Do func(string, []byte) ([]byte, error)

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
	// 把自己加到hash
	cfg.Con.Add(Member{UniqueName: fmt.Sprintf("%s://%s:%d", cfg.Protocol, cfg.Host, cfg.Port)})

	RegisterHandler(cfg, cfg.Handle)
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

	go broadcast_node.UDPDiscovery{
		Port: 9191,
		GetNewAddress: func(hostIp string, port int) {
			log.Info().Msgf("GetNewAddress#hostIp:%s,port:%d", hostIp, port)
			net.Bootstrap(fmt.Sprintf("%s://%s:%d", cfg.Protocol, hostIp, uint16(port)))
		},
		ShouldBroadcastOwnAddress: func() (int, bool) {
			if cfg.Con.Len() <= 1 {
				log.Info().Msgf("ShouldBroadcastOwnAddress#port:%d", cfg.Port)
				return int(cfg.Port), true
			}
			return 0, false
		},
	}.Loop()

	return nil
}

func RegisterHandler(cfg Config, handle HANDLE) {
	Do = func(key string, params []byte) ([]byte, error) {
		member := cfg.Con.GetNotSelf(key, Member{UniqueName: fmt.Sprintf("%s://%s:%d", cfg.Protocol, cfg.Host, cfg.Port)})
		log.Info().Msgf("to handle key=%s, member=%s", key, member)
		if member == nil {
			log.Info().Msgf("self handle key=%s", key)
			if handle == nil {
				return nil, errors.New("need register handler")
			}
			return handle.Fetch(params)
		} else {
			mem := member.(Member)
			ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*200)
			//ctx = context.Background()
			current := time.Now()
			response, err := mem.Client().Request(ctx, &req.DRequest{
				Key:    key,
				Params: []byte("get key:" + key),
			})
			log.Info().Msgf("other handle key=%s, repsonse=%s, err=%s, ts=%d", key, response, err,
				time.Since(current).Nanoseconds()/1000000)
			if err != nil {
				return nil, err
			}
			return response.(*req.DResponse).Data, nil

		}
	}
}
