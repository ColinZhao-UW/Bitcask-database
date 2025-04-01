package data

import (
	"bitcask-go/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
	dataFile3, err := OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = dataFile.Write([]byte("bbb"))
	assert.Nil(t, err)
}
