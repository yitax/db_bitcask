package main

import (
	bitcask "db-bitcask"
	bitcask_redis "db-bitcask/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	// 可以连接到多个db上去
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开redis数据库服务
	redisStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化server
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisStructure

	// 初始化redis服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)

	bitcaskServer.listen()

}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running,ready to accept connections.")
	if err := svr.server.ListenAndServe(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true

}

// 断开连接之后处理
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

// func main() {
// 	// 构造符合格式的数据
// 	// 发送给redis服务端
// 	// 接受服务端响应
// 	// 根据响应格式解析
// 	conn, err := net.Dial("tcp", "localhost:6379")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// 向redis发送命令
// 	cmd := "set key2 bitcasktest\r\n"
// 	conn.Write([]byte(cmd))

// 	// 解析Redis
// 	reader := bufio.NewReader(conn)
// 	res, err := reader.ReadString('\n')
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Println(res)
// }
