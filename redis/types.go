package redis

import (
	bitcask "db-bitcask"
	"db-bitcask/utils"
	"encoding/binary"
	"errors"
	"time"
)

type redisDataType = byte

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	// 存储转换之后redis数据结构的数据
	db *bitcask.DB
}

func NewRedisDataStructure(options bitcask.Options) (*RedisDataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// ===========================String==============================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码value=type|expire|原始value
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1

	// 如果ttl为0呢
	var expire int64
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	// PutVarint会返回长度，利用这个长度来更新index
	index += binary.PutVarint(buf[index:], expire)

	enValue := make([]byte, index+len(value))
	copy(enValue[:index], buf[:index])
	copy(enValue[index:], value)

	// 调用Put接口
	return rds.db.Put(key, enValue)

}

// 返回实际value和error
func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	enValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 对拿到的编码后的enValue进行解码
	dataType := enValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(enValue[index:])
	index += n
	// 判断是否已经过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return enValue[index:], nil

}

// ===========================Hash==============================//
// 如果field已经存在，会返回false,为了和redis接口对应
func (rds *RedisDataStructure) HSet(key []byte, field []byte, value []byte) (bool, error) {
	// 先找元数据
	meta, err := rds.findMeta(key, Hash)
	if err != nil {
		return false, nil
	}

	hik := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	// 构造数据部分的key=用户key+version+field
	// value=用户传过来的value
	enHik := hik.encode()

	// 查找数据是否存在
	// 如果不存在的话，需要更新meta的size
	var exist = true
	if _, err := rds.db.Get(enHik); err == bitcask.ErrKeyNotFound {

		exist = false
	}

	// 采用writeBatch保证原子性
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	// 不存在更新meta
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encodeMetadata())
	}

	wb.Put(enHik, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil

}

func (rds *RedisDataStructure) HGet(key []byte, field []byte) ([]byte, error) {
	// 先查找元数据是否存在
	meta, err := rds.findMeta(key, Hash)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// 组合新的key
	hik := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	// 构造数据部分的key=用户key+version+field
	// value=用户传过来的value
	enHik := hik.encode()
	return rds.db.Get(enHik)
}

func (rds *RedisDataStructure) HDel(key []byte, field []byte) (bool, error) {
	// 先获得元数据，
	meta, err := rds.findMeta(key, Hash)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 组合新的key
	hik := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	// 构造数据部分的key=用户key+version+field
	enHik := hik.encode()

	// 检查key是否存在
	var exist = true
	if _, err = rds.db.Get(enHik); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encodeMetadata())

		_ = wb.Delete(key)

		if err = wb.Commit(); err != nil {
			return false, err
		}

	}

	// 为什么这里是!exits
	return !exist, nil

}

// ===========================Set==============================//

func (rds *RedisDataStructure) SAdd(key []byte, member []byte) (bool, error) {
	// 先获得元数据，
	meta, err := rds.findMeta(key, Set)
	if err != nil {
		return false, err
	}

	// 构造key
	sik := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	enSik := sik.encode()

	var ok bool
	// 如果可以找到的话说明set中这个数据已经存在了，不用进行操作
	if _, err = rds.db.Get(enSik); err == bitcask.ErrKeyNotFound {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encodeMetadata())
		_ = wb.Put(enSik, nil)
		if err = wb.Commit(); err != nil {
			return false, nil
		}
		ok = true

	}
	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key []byte, member []byte) (bool, error) {
	// 先获得元数据，
	meta, err := rds.findMeta(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造key
	sik := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	enSik := sik.encode()

	_, err = rds.db.Get(enSik)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key []byte, member []byte) (bool, error) {
	// 先获得元数据，
	meta, err := rds.findMeta(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	// 构造key
	sik := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	enSik := sik.encode()

	_, err = rds.db.Get(enSik)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encodeMetadata())
	_ = wb.Delete(enSik)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ===========================List==============================//

func (rds *RedisDataStructure) LPush(key []byte, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key []byte, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) pushInner(key []byte, element []byte, isLeft bool) (uint32, error) {
	// 先获得元数据，
	meta, err := rds.findMeta(key, List)
	if err != nil {
		return 0, err
	}

	// 构造key
	// 左闭右开
	index := meta.head - 1
	if !isLeft {
		index = meta.tail
	}
	lik := &listInternalKey{
		key:     key,
		version: meta.version,
		index:   index,
	}

	enLik := lik.encode()

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}

	wb.Put(key, meta.encodeMetadata())
	wb.Put(enLik, element)
	if err = wb.Commit(); err != nil {
		return 0, nil
	}
	return meta.size, nil

}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

// 会返回pop出的数据
func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 先获得元数据，
	meta, err := rds.findMeta(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	var index = meta.tail - 1
	// 构造数据部分的key，头部/尾部
	if isLeft {
		index = meta.head
	}

	lik := &listInternalKey{
		key:     key,
		version: meta.version,
		index:   index,
	}

	element, err := rds.db.Get(lik.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	err = rds.db.Put(key, meta.encodeMetadata())
	if err != nil {
		return nil, err
	}
	return element, nil

}

// ===========================List==============================//
func (rds *RedisDataStructure) ZADD(key []byte, member []byte, score float64) (bool, error) {
	meta, err := rds.findMeta(key, ZSet)
	if err != nil {
		return false, err
	}

	zik := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	// 看是否已经存在
	var exist = true
	// 拿出分数来
	value, err := rds.db.Get(zik.encodeWithMember())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		exist = false
	}
	// 如果已存在，检查是否更新分数
	if exist {
		if score == utils.BytesToFloat(value) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encodeMetadata())
	}

	// 如果存在的话，还要先删除旧的key（包含score的key)
	// 只有member的key会自动覆盖
	// 如果不删除的话，旧的key也会扫描出来
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   score,
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}

	// 更新数据
	wb.Put(zik.encodeWithMember(), utils.Float64ToBytes(zik.score))
	wb.Put(zik.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, nil
	}

	return !exist, nil

}

// 拿到score
func (rds *RedisDataStructure) ZSCore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMeta(key, ZSet)
	// 目前不支持负数score
	if err != nil {
		return -1, err
	}

	if meta.size == 0 {
		return -1, nil
	}

	zik := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	// 拿出分数来
	value, err := rds.db.Get(zik.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.BytesToFloat(value), nil

}

// 查找元数据，适合多种数据类型
func (rds *RedisDataStructure) findMeta(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == bitcask.ErrKeyNotFound {
		exist = false
	} else {
		// 如果key存在，解码元数据返回
		meta = decodeMetadata(metaBuf)

		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}

	}

	// key不存在或者超时了，重新设置元数据
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			// 使用当前时间戳作为版本号
			version: time.Now().UnixNano(),
			size:    0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}

	}
	return meta, nil

}
