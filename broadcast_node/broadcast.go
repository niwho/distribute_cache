package broadcast_node

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

type Frame struct {
	Port int `json:"port"`
}

type UDPDiscovery struct {
	Port                      int
	GetNewAddress             func(string, int)
	ShouldBroadcastOwnAddress func() (int, bool)
}

// 返回新发现的地址
func (up UDPDiscovery) ListenUdp() {
	pc, err := net.ListenPacket("udp", fmt.Sprintf(":%d", up.Port))
	if err != nil {
		fmt.Println(err)
		return
	}
	var frame Frame
	for {
		buffer := make([]byte, 10240) //10k
		n, addr, err := pc.ReadFrom(buffer)
		buffer = buffer[:n]
		if err == nil {
			hostIp := strings.Split(addr.String(), ":")[0]
			err := json.Unmarshal([]byte(string(buffer)), &frame)
			fmt.Println("UDPDiscovery", hostIp, string(buffer), err)
			up.GetNewAddress(hostIp, frame.Port)
		}
	}
}

// 自己没有任何连接时，每5s发一次关播消息
func (up UDPDiscovery) Loop() {
	go up.ListenUdp()
	for {
		time.Sleep(5 * time.Second)
		if port, ok := up.ShouldBroadcastOwnAddress(); ok {
			con, err := net.DialTimeout("udp", fmt.Sprint("255.255.255.255:", up.Port), time.Second*60)
			if err != nil {
				fmt.Printf("dial err=%+v", err)
				continue
			}
			dat, _ := json.Marshal(Frame{Port: port})
			con.Write(dat)
		}
	}
}
