package data

import (
	"bitcask-go/fio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value,log record maybe corrupted")
)

type DataFile struct {
	FileId    uint32        //文件id
	WriteOff  int64         //文件写到了哪个位置
	IoManager fio.IOManager //io读写管理
}

// crc type keySize valueSize
// 4 	1	5		5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 打开新的数据文件，返回一个对应的dataFile文件
// 获取这个file的name
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId, ioType)
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	//需要传入路径+文件名
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}
func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	//
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	//fmt.Sprintf("%09d"，filedId)，将fileId变成一个九位数，使用0去补全一个数
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// 根据offset，从数据文件读取LogRecord
// 将位置信息转化为对应的LogRecord对象
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	//如果读取的最大header长度已经超过了文件的长度，则只需要读到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	// 读取头，headerBytes表示需要读取多长的内容
	// 返回值是一个byte类型数组，命名为headerBuf
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	// 解码headerBuf中的内容,即前headerBytes的内容，返回一个LogRecordHeader对象和整个header的大小
	// 将byte[]信息转化为一个实际的header对象
	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 下面的两个条件表示读区到了文件末尾，直接返回eof错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 读完头后，从头里取出对应key和value长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}

	// 开始读取用户实际存储的key/value数据
	if keySize > 0 || valueSize > 0 {
		// 从 offset + headerSize这个位置开始进行读取 keySize + valueSize这么大的数据
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 取出value和size
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// 根据取出来的数据进行crc计算，观察和header里的crc是否相同
	crc := getLogRecordCrc(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = df.IoManager.Read(buf, offset)
	return
}
