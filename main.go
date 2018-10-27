package main

import (
	"bufio"
	"flag"
	"os"
	"strings"

	"context"
	"fmt"
	"github.com/niwho/distribute_cache/consistent"
	"github.com/niwho/distribute_cache/exchange"
	"github.com/niwho/distribute_cache/proto"
	"github.com/perlin-network/noise/log"
)

func main() {
	// process other flags
	portFlag := flag.Int("port", 3000, "port to listen to")
	hostFlag := flag.String("host", "localhost", "host to listen to")
	protocolFlag := flag.String("protocol", "tcp", "protocol to use (kcp/tcp)")
	peersFlag := flag.String("peers", "", "peers to connect to")
	flag.Parse()

	port := uint16(*portFlag)
	host := *hostFlag
	protocol := *protocolFlag
	peers := strings.Split(*peersFlag, ",")

	cons := consistent.NewDistrubuteConsistent()
	err := exchange.Listen(exchange.Config{
		Protocol:       protocol,
		Host:           host,
		Port:           port,
		BootstrapPeers: peers,
		Con:            cons,
	})
	if err != nil {
		log.Error().Msgf("err:%s", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')

		// skip blank lines
		if len(strings.TrimSpace(input)) == 0 {
			continue
		}

		member := cons.GetNotSelf(input, exchange.Member{UniqueName: fmt.Sprintf("%s://%s:%d", protocol, host, port)})
		if member == nil {
			log.Info().Msgf("self handle key=%s", input)
		} else {
			mem := member.(exchange.Member)
			response, err := mem.Client().Request(context.Background(), &req.DRequest{
				Params: []byte("get key:" + input),
			})
			log.Info().Msgf("self handle key=%s, repsonse=%s, err=%s", input, response, err)
		}

	}

}
