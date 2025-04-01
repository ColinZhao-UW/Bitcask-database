package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// 抽象索引接口。目前使用btree
type Indexer interface {
	//向索引中管理key对应的位置信息
	//pos就是在文件存储的位置信息
	//put的语义：将student:1 插入到文件1，偏移量为100的地方
	//Indexer.Put([]byte("student:1",&data.LogRecordsPos{Fid:1,Offset:100}))
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	//迭代器
	Iterator(reverse bool) Iterator
	Size() int
}

type IndexType = int8

const (
	//Btree索引
	Btree IndexType = iota + 1
	//Art自适应索引书
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("unknown index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// 抽象出索引迭代器接口，这样Indexer的数据结构变更后，只需要去实现接口就行。
type Iterator interface {
	//回到迭代器起点
	Rewind()
	//查找到第一个大于或小于目标的key，从这个key开始遍历
	Seek(key []byte)
	//跳到下一个key
	Next()
	//判断是否有效，即是否遍历完了所有的key，用于退出遍历
	Valid() bool
	//取出迭代器当前位置的key
	Key() []byte
	//取出当前位置的value
	Value() *data.LogRecordPos
	//关闭迭代器
	Close()
}
