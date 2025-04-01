package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// bitcask存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	index      index.Indexer
	fileIds    []int //文件id，只用于加载索引的时候使用，不能用于其他地方的更新和使用
	seqNo      uint64
	isMerging  bool //db是否在进行mer操作

}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDatabaseDirPathEmpty
	}
	if options.DataFileSize <= 0 {
		return ErrDatabaseFileSizeInvalid
	}
	return nil
}

// 打开引擎实例
func Open(options Options) (*DB, error) {
	//对配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	//对用户传入的目录进行校验，如果目录不存在就创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化db实例

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	//加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//从hint索引文件加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.loadDbFiles(); err != nil {
		return nil, err
	}

	//从数据文件加载索引
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}
	return db, nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 写入Key/value数据，key不能为空 p5
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//构造LogRecords结构体
	logRecord := &data.LogRecord{
		Key:   logRecordWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	//追加写入到当前活跃文件中，这个方法会返回一个pos拿来当索引存储
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 根据key读取数据, p5
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//从内存数据中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	return db.getValueByPosition(logRecordPos)
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写记录到活跃文件中， p5
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	//判断当前活跃数据文件是否存在
	//如果为空则初始化数据
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//需要有一个编码方法
	//写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	//如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//先持久化数据文件，保证以后的数据持久到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//将当前的活跃文件转化为旧的文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//在索引中的writeOff是写入之前的，这样可以读到起始位置，
	//再读size个字符就表示将这个数据读完了
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//判断是否需要对数据进行持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//构造pos数据返回给put方法，写入内存
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, db.options.IOType)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 加载数据，将数据文件加载到db中去
func (db *DB) loadDbFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//遍历所有文件名，找到以data结尾的文件，将所有id加到一个集合中
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			//strconv.Atoi是ascii码转int的意思，将字符串转化为int类型
			fileId, err := strconv.Atoi(splitNames[0])
			//数据目录可能损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	//对id进行排序，保证索引操作时可以按顺序打开id文件
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), db.options.IOType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			//最后一个id是最大的，标记为活跃文件
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFile() error {
	//没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}

		if !ok {
			panic("failed to update index at start")
		}
	}

	//暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo
	//遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		//如果比最近来参与merge的文件id更小，则说明已经从hint文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		//循环处理文件中的内容
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//构建内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 需要对读出的logRecordKey进行解码，因为我们之前放入了序列号信息
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				//非事务操作
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//是事务操作,且是事务完成的操作
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			//更新序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			//递增offset，下一步从新的位置开始读区
			offset += size
		}

		//如果是当前活跃文件，更新这个文件的offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	//更新事务序列号
	db.seqNo = currentSeqNo
	return nil

}

// 删除key对应的数据
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造logRecord表示其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	//从内存索引中删除对应的key
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	//根据文件id找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	//检查文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//判断这条记录是不是被删除的
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrDataFileNotFound
	}
	return logRecord.Value, nil
}

// 获取数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}
