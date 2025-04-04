package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// bitcask存储引擎实例
type DB struct {
	options     Options
	mu          *sync.RWMutex
	activeFile  *data.DataFile
	olderFiles  map[uint32]*data.DataFile
	index       index.Indexer
	fileIds     []int //文件id，只用于加载索引的时候使用，不能用于其他地方的更新和使用
	seqNo       uint64
	isMerging   bool //db是否在进行mer操作
	fileLock    *flock.Flock
	byteWrite   uint
	reclaimSize int64 //表示有多少数据是无效的
}

type Stat struct {
	KeyNum          uint  //key的数量
	DataFileNum     uint  //文件数量
	ReclaimableSize int64 //无效数据量
	DiskSize        int64 //磁盘大小
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDatabaseDirPathEmpty
	}
	if options.DataFileSize <= 0 {
		return ErrDatabaseFileSizeInvalid
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return ErrMergeRatioError
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

	//判断当前数据目录是否在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}

	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	//初始化db实例

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
		fileLock:   fileLock,
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

	//需要将io类型重置
	if db.options.MMapAtStartUp {
		if err := db.resetIoType(); err != nil {
			return nil, err
		}
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

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
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
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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
	// 判断当前活跃数据文件是否存在
	// 如果为空则初始化数据
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 存入文件的是要被编码后的数据，调用encodeLogRecord对数据进行编码
	// 将数据插入到对应文件中，同时保证了如果当前文件写不下，会开启新的文件写入，保证数据一定会成功写入到一个文件中
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//先持久化数据文件，保证oldFiles里的文件全部是已经持久化过的数据。
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前的活跃文件转化为旧的文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 在索引中的writeOff保留的是这条记录的起始地址，这样可以读到起始位置，
	// 这个writeOff需要记录到索引信息中
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 判断是否需要对数据进行立刻的持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.byteWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//清空累积值
		if db.byteWrite > 0 {
			db.byteWrite = 0
		}
	}

	// 构造pos数据返回给put方法，写入内存
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size)}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
// 该方法在以下几种情况会被调用：
// 1.文件被写满。2.数据库启动时为空
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	//打开一个文件，即获取到一个dataFile对象
	//调用dataFile对象即可操作这个具体的文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
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
		ioType := fio.StandardFIO
		if db.options.MMapAtStartUp {
			ioType = fio.MemoryMap
		}
		//如果是mmap就使用mmap类型来进行读数据
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
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
// 文件是已经从小到大排列的，所以只需要按从小到大的顺序读取文件，即能获取整个数据库的执行流程。
// 因为记录是一直插入的，所以我们事务不需要对loadFiles做额外处理，记录已经写到了对应的file里
// 但是事务确实需要对索引进行额外处理，保证你启动数据库的时候，能够将全部的索引正确加载。
func (db *DB) loadIndexFromDataFile() error {
	//没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否有merge标识文件
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	//有的话就需要获取现在没被merge过的最小的文件id
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	//定义一个更新索引的方法
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}

		if oldPos == nil {
			panic("failed to update index at start")
		} else {
			db.reclaimSize += int64(pos.Size)
		}
	}

	// 暂存事务数据的一个缓冲区，uint64是key，TransactionRecord[]是对应的value
	// 语义为，一个事务id对应一组含n个 包含索引和record的 TransactionRecord
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	// 只处理id比nonMergeFileId大的文件，小的直接忽略
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近来参与merge的文件id更小，则说明已经从hint文件中加载索引了。
		// 则不需要做额外的处理
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 循环处理文件中的内容
		// 拿到所有的logRecord
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 构建这条record对应的内存索引
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 需要对读出的logRecordKey进行解码，因为我们之前放入了序列号信息
			// 通过序列号可以判定这个record是否为一个事务
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				//非事务操作，则可以直接进行修改索引操作
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 是事务操作,且是事务完成的操作
				// 将缓冲区中的信息读出来，修改对应的索引信息。
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					// 执行结束后将对应的缓冲信息删除
					delete(transactionRecords, seqNo)
				} else {
					// 是事务操作，但不是事务完成的操作，即这条记录是一条普通事务操作
					logRecord.Key = realKey
					// 将对应的记录插入到缓冲池中，为事务完成操作做准备
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
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}
	db.reclaimSize += int64((pos.Size))
	//从内存索引中删除对应的key
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	//根据文件id获取对应的dataFile对象
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
	//判断这条记录是不是delete类型，如果是，表示这条记录是被删除的，需要返回空
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

// 将数据文件的 IO 类型设置为标准文件 IO，14课时加入
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
