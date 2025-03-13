package redis

import "errors"

// 存放通用命令

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	// 先拿到数据
	enValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(enValue) == 0 {
		return 0, errors.New("value is null")
	}

	// 对拿到的编码后的enValue进行解码
	dataType := enValue[0]
	return dataType, nil
}
