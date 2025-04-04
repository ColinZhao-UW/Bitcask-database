package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// 查找到第一个大于或小于目标的key，从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// 跳到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// 判断是否有效，即是否遍历完了所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// 取出迭代器当前位置的key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// 取出当前位置的value
func (it *Iterator) Value() ([]byte, error) {
	//拿到的是位置索引信息，通过这个信息取拿Value
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// 关闭迭代器
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 跳到下一个指定前缀的数据
// 比如配置的制定前缀为[]byte("b")，那么就会跳过索引的apple
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
