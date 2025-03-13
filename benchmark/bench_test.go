package benchmark

import (
	bitcask "db-bitcask"
	"db-bitcask/utils"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// put,get,delete都需要使用
var db *bitcask.DB

func init() {

	// 初始化存储引擎对象
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-filelock")
	opts.DirPath = dir
	var err error
	db, err = bitcask.Open(opts)
	if err != nil {
		panic(err)
	}
}

// 使用B
func Benchmark_Put(b *testing.B) {

	// 忽略准备工作的耗时
	b.ResetTimer()
	// 内存分配情况
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {

		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {

	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)

	}
	rand.Seed(time.Now().UnixNano())
	// 忽略准备工作的耗时
	b.ResetTimer()
	// 内存分配情况
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}

}

func Benchmark_Delete(b *testing.B) {

	// 忽略准备工作的耗时
	b.ResetTimer()
	// 内存分配情况
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {

		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
