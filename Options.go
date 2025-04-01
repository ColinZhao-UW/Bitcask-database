package bitcask_go

import "os"

// 配置项
type Options struct {
	DirPath      string //数据库数据目录
	DataFileSize int64  //数据文件大小
	SyncWrites   bool   //每次写入是否需要持久化
	IndexType    IndexType
	IOType       FileIOType
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
)

type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	//一个批次插入的最大数据量
	MaxBatchNum uint

	//提交事务时是否需要持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  false,
}
