package fio

import (
	"errors"
)

const DataFilePerm = 0644

type IOManager interface {
	//从文件的指定位置读取对应数据
	//即byte为文件名，int64为偏移量
	Read([]byte, int64) (int, error)
	//向指定文件名写入数据，我们是顺序写，所以直接到末尾写
	Write([]byte) (int, error)
	//持久化数据
	Sync() error
	//关闭文件
	Close() error
	//获取文件大小
	Size() (int64, error)
}

type FileIOType = byte

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

func NewIOManager(fileName string, typ FileIOType) (IOManager, error) {
	switch typ {

	case StandardFIO:
		return NewFileIOManager(fileName)

	case MemoryMap:
		return NewMMapIOManager(fileName)

	default:
		return nil, errors.New("Invalid IO type")
	}

}
