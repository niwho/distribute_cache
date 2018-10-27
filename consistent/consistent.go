package consistent

import (
	"stathat.com/c/consistent"
	"sync"
)

type IMember interface {
	String() string
}

type ICONSISTENT interface {
	Add(member IMember)                          // 增加节点
	Remove(member IMember)                       // 释放节点
	Get(key string) IMember                      // 获取hash后的节点，一致性
	GetNotSelf(key string, self IMember) IMember // 获取hash后的节点，一致性, 非自己
}

func NewDistrubuteConsistent() *DistributeConsistent {
	return &DistributeConsistent{
		Consistent: consistent.New(),
	}
}

type DistributeConsistent struct {
	*consistent.Consistent
	sync.Map
}

func (dc *DistributeConsistent) Add(member IMember) {
	dc.Store(member.String(), member)
	dc.Consistent.Add(member.String())
}

func (dc *DistributeConsistent) Remove(member IMember) {
	dc.Delete(member.String())
	dc.Consistent.Remove(member.String())

}

func (dc *DistributeConsistent) Get(key string) IMember {
	c, _ := dc.Consistent.Get(key)
	val, ok := dc.Map.Load(c)
	if !ok {
		return nil
	}
	// should be this type or panic
	return val.(IMember)
}

func (dc *DistributeConsistent) GetNotSelf(key string, self IMember) IMember {
	mem := dc.Get(key)
	if mem.String() != self.String() {
		return mem
	}
	return nil
}
