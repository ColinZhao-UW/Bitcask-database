package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

var txnFinKey = []byte("txn-fin")

const nonTransactionSeqNo uint64 = 0

type WriteBatch struct {
	db            *DB
	mu            *sync.RWMutex
	pendingWrites map[string]*data.LogRecord //暂存的记录，string类型是可比较的，而[]byte类型不可比较，所以map都定义为string
	options       WriteBatchOptions
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		db:            db,
		mu:            new(sync.RWMutex),
		pendingWrites: make(map[string]*data.LogRecord),
		options:       opts,
	}
}

// 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	//暂存logRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	//数据在内存中不存在
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
	}
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 提交事务
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	//缓冲区内长度大于配置项设置的最大长度
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	//获取当前序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	//开始写入事务操作到数据库中
	//在这一步中将每个在pendingWrites中的记录写到具体文件中，并返回对应的索引
	//position是索引的一个集合，将所有索引都加入到position集合中
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	//写一条标识事务内数据全部插入数据库的记录
	finishedRecord := &data.LogRecord{
		Key:  logRecordWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//根据配置进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新内存索引
	//因为需要record的type信息，所以遍历的是pendingWrites而不是positions
	//通过遍历每个pendingWrites，拿到对应的key，然后去position里查。
	//之后将position里的索引插到实际数据库的索引中。
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	//清空缓存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// key+seq编码
func logRecordWithSeq(key []byte, seqNo uint64) []byte {
	//将seq进行变长编码，将他和key一起写入
	seq := make([]byte, binary.MaxVarintLen64)
	//返回的是seq编码后的长度
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析logRecord中的key，获取实际的key和序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
