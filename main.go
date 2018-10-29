package main

import (
	"bufio"
	"flag"
	"os"
	"strings"

	"fmt"
	"github.com/niwho/distribute_cache/consistent"
	"github.com/niwho/distribute_cache/exchange"
	"github.com/perlin-network/noise/log"
	"time"
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
		Handle:         TestHandle{},
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

		resp, err := exchange.Do(input, []byte("test data"))
		log.Info().Msgf("resp:%s, err:%s", string(resp), err)

	}

}

type TestHandle struct {
}

func (TestHandle) Fetch(params []byte) (resp []byte, err error) {
	log.Info().Msgf("params:%s", string(params))
	resp = []byte(fmt.Sprint("%d", time.Now().Unix()))
	return
}
