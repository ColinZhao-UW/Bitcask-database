package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

type BTree struct {
	//写并发不安全，读安全
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// 这也是一种初始化方式，直接输入结构体加给结构体里面赋值，返回的是一个对象，使用&将地址取出来
func NewBTreeAnother() *BTree {
	bt := BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
	return &bt
}

// 这个也是一种初始化方法
func NewBTreeWithNew() *BTree {
	bt := new(BTree)            // 分配内存，所有字段为零值，返回的是指针
	bt.tree = btree.New(32)     // 手动初始化 tree
	bt.lock = new(sync.RWMutex) // 手动初始化 lock
	return bt
}
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// 迭代器结构体
type btreeIterator struct {
	curIndex int     //当前遍历的下标
	reverse  bool    //是否反向遍历
	values   []*Item // key位置的索引信息
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

// 回到迭代器起点
func (bti *btreeIterator) Rewind() {
	bti.curIndex = 0
}

// 查找到第一个大于或小于目标的key，从这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// 跳到下一个key
func (bti *btreeIterator) Next() {
	bti.curIndex++
}

// 判断是否有效，即是否遍历完了所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

// 取出迭代器当前位置的key
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

// 取出当前位置的value
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

// 关闭迭代器
func (bti *btreeIterator) Close() {
	bti.values = nil
}
