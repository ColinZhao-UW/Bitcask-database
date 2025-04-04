package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// 头的定义
type LogRecordHeader struct {
	crc        uint32        // 校验值
	recordType LogRecordType //记录类型
	keySize    uint32        // key的大小
	valueSize  uint32        // value的大小
}

// 索引结构体
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
	Size   uint32 // 标识数据在磁盘上的大小
}

// 写入数据文件的记录结构体
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 对logRecord进行编码，返回字节数组及其长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//初始化一个header部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	//第五个字节存储type
	header[4] = logRecord.Type
	var index = 5

	//5字节之后，存储key和value的长度信息
	//使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)

	encBytes := make([]byte, size)

	//将header部分拷贝过来
	copy(encBytes[:index], header[:index])

	//将key和value拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	//对整个logRecord数据进行crc校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	//将crc填充到encBytes中
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	//fmt.Printf("header length : %d,crc : %d\n", index, crc)

	return encBytes, int64(size)
}

// 头部解码
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	//取出这个byte数组中的前五位，分别是crc校验和type
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	//取出buf中的keySize
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	//取出实际的valueSize
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

func getLogRecordCrc(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}

// 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// 对位置信息进行解码
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	filedId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(filedId),
		Offset: offset,
		Size:   uint32(size),
	}
}

// 暂存事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}
