package redis

import (
	bitcask "db-bitcask"
	"db-bitcask/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(val1))

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	t.Log(string(val2))

}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(11))
	t.Log(err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	t.Log(err)
}

func TestRedisDataStructure_HSet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok1)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok3)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(100))
	t.Log(ok2)

}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	v1 := utils.RandomValue(100)
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	t.Log(ok1)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v2)
	t.Log(ok3)

	v3 := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v3)
	t.Log(ok2)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	t.Log(string(v1))
	t.Log(string(v2))
	t.Log(string(val1))

}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-redis-set")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err1 := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err1)
	t.Log(ok1)

	ok2, err2 := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err2)
	t.Log(ok2)

	ok3, err3 := rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err3)
	t.Log(ok3)

	ok4, err4 := rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err4)
	t.Log(ok4)

	ok5, err5 := rds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err5)
	t.Log(ok5)

	ok6, err6 := rds.SIsMember(utils.GetTestKey(1), []byte("val-5"))
	assert.Nil(t, err6)
	t.Log(ok6)

	ok7, err7 := rds.SRem(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err7)
	t.Log(ok7)

	ok4, err4 = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err4)
	t.Log(ok4)

}

func TestRedisDataStructure_POP_PUSH(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-redis-list")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ele, err := rds.LPop(utils.GetTestKey(1))
	t.Log(ele)
	t.Log(err)

	size, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	t.Log(size)

	size, err = rds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	t.Log(size)

	size, err = rds.RPush(utils.GetTestKey(1), []byte("val-3"))
	assert.Nil(t, err)
	t.Log(size)

	elem, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(elem))

	elem, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(elem))
}

func TestRedisDataStructure_Zset(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "db-bitcask-redis-zset")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.ZADD(utils.GetTestKey(1), []byte("val-5"), 5)
	t.Log(ok)
	assert.Nil(t, err)

	ok, err = rds.ZADD(utils.GetTestKey(1), []byte("val-5"), 3)
	t.Log(ok)
	assert.Nil(t, err)

	ok, err = rds.ZADD(utils.GetTestKey(1), []byte("val-3"), 3)
	t.Log(ok)
	assert.Nil(t, err)

	score, err := rds.ZSCore(utils.GetTestKey(1), []byte("val-5"))
	t.Log(score)
	assert.Nil(t, err)
}
