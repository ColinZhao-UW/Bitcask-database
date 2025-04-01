package data

import (
	"bitcask-go/fio"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"os"
	"testing"
)

func TestEncodeLogRecor(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log(res1)
	t.Log(n1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	//value为空的情况
	res2, n2 := EncodeLogRecord(rec2)
	t.Log(res2)
	t.Log(n2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	//对deleted情况的测试

}

// 936988949
func TestDecodeLogRecord(t *testing.T) {
	headerBuf1 := []byte{21, 81, 217, 55, 0, 8, 10}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	t.Log(h1)
	t.Log(size1)

}

func TestGetLogRecord(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log(res1)
	t.Log(n1)
	headerBuf1 := []byte{21, 81, 217, 55, 0, 8, 10}
	crc1 := getLogRecordCrc(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(936988949), crc1)
}
func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 6666, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有一条 LogRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(readSize1)

	// 多条 LogRecord，从不同的位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
}
