package exchange

import (
	"github.com/niwho/distribute_cache/consistent"
	"github.com/niwho/distribute_cache/proto"
	"github.com/perlin-network/noise/log"
	"github.com/perlin-network/noise/network"
	"github.com/perlin-network/noise/types/opcode"
)

func init() {
	opcode.RegisterMessageType(opcode.Opcode(1000), &req.DRequest{})
	opcode.RegisterMessageType(opcode.Opcode(1001), &req.DResponse{})
}

type Member struct {
	UniqueName string
	client     *network.PeerClient
}

func (m Member) String() string {
	return m.UniqueName
}
func (m Member) Client() *network.PeerClient {
	return m.client
}

type ExchangeData struct {
	*network.Plugin
	handle HANDLE
	con    consistent.ICONSISTENT
}

// Callback for when the network starts listening for peers.
func (ex ExchangeData) Startup(net *network.Network) {
	log.Info().Msgf("Startup :%s", net.Address)
	// 保持一致性，必须把自己也加到组内
	ex.con.Add(Member{UniqueName: net.Address})
}

// Callback for when an incoming message is received. Return true
// if the plugin will intercept messages to be processed.
func (ex ExchangeData) Receive(ctx *network.PluginContext) error {
	// 解析请求参数
	switch msg := ctx.Message().(type) {
	case *req.DRequest:
		log.Info().Msgf("<%s>", ctx.Client().Address)
		// 先判断是否直接处理这个请求 还是转发

		//log.Info().Msgf("handle key:%s", msg.Key)
		var data []byte
		var err error
		if ex.handle != nil {
			data, err = ex.handle.Fetch(msg.Params)
		} else {
			data = []byte("empty")
		}
		ctx.Reply(&req.DResponse{
			Meta:  &req.Meta{},
			ReqId: msg.ReqId,
			Data:  data,
		})
		return err

	}
	return nil
}

// Callback for when the network stops listening for peers.
func (ex ExchangeData) Cleanup(net *network.Network) {

}

// Callback for when a peer connects to the network.
func (ex ExchangeData) PeerConnect(client *network.PeerClient) {
	// 收到其它节点连接
	// 一致性hash更新
	log.Info().Msgf("PeerConnect:%v", client.Address)
	ex.con.Add(Member{client.Address, client})
}

// Callback for when a peer disconnects from the network.
func (ex ExchangeData) PeerDisconnect(client *network.PeerClient) {
	// 失去连接
	// 一致性hash更新
	ex.con.Remove(Member{client.Address, client})
}
