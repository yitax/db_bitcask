package redis

import (
	"db-bitcask/utils"
	"encoding/binary"
	"math"
)

const (
	maxMetaSize       = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = 2 * binary.MaxVarintLen64

	initialListMark = math.MaxUint64 / 2
)

// 元数据，主要用于保存数据整体的一些信息
// hash set zset list
type metadata struct {
	dataType byte
	expire   int64
	version  int64
	size     uint32
	head     uint64
	tail     uint64
}

// 类似于logrecord的编码
func (md *metadata) encodeMetadata() []byte {
	var size = maxMetaSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType

	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]

}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n

		tail, n = binary.Uvarint(buf[index:])
		index += n
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}

}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hik *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hik.key)+len(hik.field)+8)
	var index = 0
	copy(buf[index:index+len(hik.key)], hik.key)
	index += len(hik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hik.version))
	index += 8

	copy(buf[index:index+len(hik.field)], hik.field)

	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
	// 进行编码的时候还需要加上size,获取member内容时需要
	// 但这里不用加size参数
}

func (sik *setInternalKey) encode() []byte {
	buf := make([]byte, len(sik.key)+8+len(sik.member)+4)
	var index = 0
	copy(buf[index:index+len(sik.key)], sik.key)
	index += len(sik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sik.version))
	index += 8

	copy(buf[index:index+len(sik.member)], sik.member)
	index += len(sik.member)

	// member长度
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sik.member)))
	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lik *listInternalKey) encode() []byte {
	buf := make([]byte, len(lik.key)+16)

	var index = 0
	copy(buf[index:], lik.key)
	index += len(lik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lik.version))
	index += 8

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lik.index))

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

// 两种编码

func (zik *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zik.key)+8+len(zik.member))

	var index = 0
	copy(buf[index:], zik.key)
	index += len(zik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zik.version))
	index += 8

	// member
	copy(buf[index:index+len(zik.member)], zik.member)
	index += len(zik.member)

	return buf

}

func (zik *zsetInternalKey) encodeWithScore() []byte {
	enScore := utils.Float64ToBytes(zik.score)
	buf := make([]byte, len(zik.key)+8+len(zik.member)+len(enScore)+4)

	var index = 0
	copy(buf[index:], zik.key)
	index += len(zik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zik.version))
	index += 8

	// score
	copy(buf[index:index+len(enScore)], enScore)
	index += len(enScore)
	// member
	copy(buf[index:index+len(zik.member)], zik.member)
	index += len(zik.member)

	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(zik.member)))

	return buf
}
