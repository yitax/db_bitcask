package main

import (
	bitcask "db-bitcask"
	bitcask_redis "db-bitcask/redis"
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

// 定义处理函数的方法
type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":  set,
	"get":  get,
	"hset": hset,
	"sadd": sadd,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command: ' " + command + " ' ")
		return
	}
	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)

	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("set")
	}

	// set a 100
	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	// log.Println("set ok")
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("get")
	}

	//  get a
	key := args[0]
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("hset")
	}

	key := args[0]
	field := args[1]
	value := args[2]
	var ok = 0
	res, err := cli.db.HSet(key, field, value)
	if res {
		ok = 1
	}
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("sadd")
	}

	key := args[0]
	member := args[1]

	var ok = 0
	res, err := cli.db.SAdd(key, member)
	if res {
		ok = 1
	}
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}

	key := args[0]
	value := args[1]

	res, err := cli.db.LPush(key, value)

	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}
