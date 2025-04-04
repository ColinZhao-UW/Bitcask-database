package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"

	//"bitcask-go/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
	// 如果数据库为空，则直接返回
	if db.activeFile == nil {
		return nil
	}
	// 上锁是为了锁定当前的所有文件，避免在拿到所有文件之前有新的数据的增删操作
	// 当拿到所有的datafile文件之后，就可以解锁。
	db.mu.Lock()
	// 如果 merge 正在进行当中，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	// 查看可以merge的数据是否达到了阈值
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioError
	}

	// 查看磁盘剩余空间能否进行merge
	availableSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	// 当前目录大小，减去操作数量，可以得到merge的文件量
	// merge量大于磁盘可用量就代表不能执行merge操作
	if uint64(totalSize-db.reclaimSize) >= availableSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true

	defer func() {
		db.isMerging = false
	}()

	//将当前活跃文件的缓冲内容写入磁盘
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	//将当前活跃文件转化为旧的数据文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	//打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}

	//记录最近没有参与merge的文件id
	nonMergeFileId := db.activeFile.FileId

	//取出所有需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	//对文件的id进行排序，为了后面的从小到大遍历
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	//如果之前已经发生过了merge，就将之前merge的结果删掉。
	mergePath := db.getMergePath()
	//err == nil说明这个过程中无报错，证明merge目录是存在的，说明发生过merge，将要将其删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//打开一个新的临时bitcask实例
	mergeOptions := db.options

	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	//打开一个新的存储引擎实例
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	//获取一个负责管理hintFile的DataFile，用来存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//遍历每个数据文件
	for _, dataFile := range mergeFiles {
		//从偏移量为0开始读
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//获取这个record在索引中的pos类型的数据
			//读记录本身就已经有了这个记录的具体位置，只需要去和内存中的位置进行比对
			//如果不同，说明这条记录已经过期了
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)

			// 和内存中的索引位置进行比较，如果有效则重写
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// 清除事务标记，
				// merge之后已经不需要事务这种多余信息了，因为merge等于对之前的数据进行一次规整
				logRecord.Key = logRecordWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将当前位置索引写到Hint文件当中
				// hint文件 存储的是这条记录的索引。
				// 即我们之后要将hintFile的内容加入到索引中。
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key: []byte(mergeFinishedKey),
		//int -> ascii
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	//db.options.Dirpath = "/path/to/db"
	//path.Clean("/path/to/db") -> /path/to/db
	//path.Dir("/path/to/db") -> /path/to
	//path.Base("/path/to/db") -> db
	//base + mergeDirName -> db-merge
	//filepath.Join("/path/to", "db-merge") -> /path/to/db-merge
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

// 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// merge 目录不存在的话直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 查找标识 merge 完成的文件，判断 merge 是否处理完了
	// 同时将当前文件夹中的所有文件都加入到mergeFileNames集合中
	// 如果发现mergeFinished确实完成了，那么这些mergeFileNames中的文件最终会加入到原路径中
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	//	没有merge完成则直接返回
	if !mergeFinished {
		return nil
	}
	// 获取最大未被merge的文件id
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	// 删除旧的数据文件
	var fileId uint32 = 0
	// 从小到大遍历现在的所有oldDataFile，将merge过的文件全部删除
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		//如果得到了这个id的文件，就将它删除
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		desPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, desPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	//	打开 hint 索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	// 读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解码拿到实际的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size

	}
	return nil
}
