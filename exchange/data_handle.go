package exchange

// 业务接口
type HANDLE interface {
	Fetch(params []byte) ([]byte, error)
}
