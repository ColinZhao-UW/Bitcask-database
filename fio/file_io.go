package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// 即byte为缓冲区，int64为偏移量
// 返回值是读到的长度
func (fio *FileIO) Read(buf []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(buf, offset)
}

// 将缓冲区内的数据写入，我们是顺序写，所以直接到末尾写
// 操作系统的write就是在末尾追加写
// 返回值是写入的长度
func (fio *FileIO) Write(buf []byte) (int, error) {
	return fio.fd.Write(buf)
}

// 持久化数据
// 正常的write会将数据写入缓冲区，在一个合适的时候写入硬盘
// 每个文件有一个缓冲区
// 如果调用sync会立刻将缓冲区里的数据写入硬盘
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
