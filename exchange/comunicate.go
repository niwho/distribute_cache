package exchange

import (
	"context"
	"errors"
	"github.com/gogo/protobuf/proto"
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
func (ExchangeData) Startup(net *network.Network) {

}

// Callback for when an incoming message is received. Return true
// if the plugin will intercept messages to be processed.
func (ex ExchangeData) Receive(ctx *network.PluginContext) error {
	// 解析请求参数
	switch msg := ctx.Message().(type) {
	case *req.DRequest:
		log.Info().Msgf("<%s> %s", ctx.Client().Address)
		// 先判断是否直接处理这个请求 还是转发
		member := ex.con.GetNotSelf(msg.Key, Member{UniqueName: ctx.Self().Address})
		if member == nil {
			// 自己处理
			log.Info().Msgf("handle key:%s", msg.Key)
			if ex.handle != nil {
				data, err := ex.handle.Fetch(msg.Params)
				ctx.Reply(&req.DResponse{
					Meta:  &req.Meta{},
					ReqId: msg.ReqId,
					Data:  data,
				})
				return err
			} else {
				log.Error().Msg("handle empty")
			}
		} else {
			var data []byte
			var err error
			if mymem, ok := member.(Member); ok {
				var response proto.Message
				response, err = mymem.client.Request(context.Background(), msg)
				if err != nil {
					// todo 根据错误类型判断是否重试,只有网络错误才重试
					//本地尝试获取一次
					data, err = ex.handle.Fetch(msg.Params)
				} else {
					// forward的结果
					resp := response.(*req.DResponse)
					data = resp.Data
				}
			} else {
				// self fetch
				data, err = ex.handle.Fetch(msg.Params)
			}

			ctx.Reply(&req.DResponse{
				Meta:  &req.Meta{},
				ReqId: msg.ReqId,
				Data:  data,
			})
			return err

		}
	default:
		return errors.New("type error: should be DRequest")
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
