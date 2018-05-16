package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gomodule/redigo/redis"
)

/*
func recvReload() error {
	conn, err := redis.Dial("tcp", redis_conn)
	if err != nil {
		return err
	}
	defer conn.Close()

	psc := redis.PubSubConn{conn}
	psc.Subscribe("server.reload.ans")
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			log.Printf("%s: message: %s\n", v.Channel, v.Data)
			return nil
		case error:
			log.Println("redis subscribe error!", v)
			return v
		}
	}
}
*/

type gmCmd struct {
	Cmd    string `json:"cmd"`
	Target string `json:"target"`
}

func sendReload(msgList []string, server *RedisConf) error {

	// go recvReload()

	conn, err := redis.Dial("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("redis connect fail addr=%s err=%s", server.Addr, err)
	}
	defer conn.Close()

	if server.Db > 0 {
		if _, err := conn.Do("SELECT", server.Db); err != nil {
			return fmt.Errorf("redis select db fail db=%d err=%s", server.Db, err)
		}
	}

	for _, m := range msgList {
		cmd := &gmCmd{
			Cmd:    "reload",
			Target: m,
		}
		data, _ := json.Marshal(cmd)

		ret, err := redis.Int(conn.Do("PUBLISH", "server.cmd", data))
		if err != nil {
			return fmt.Errorf("redis reload message publish fail! msg=%s err=%s", string(data), err)
		}
		log.Println("send reload command to server! msg=", string(data), ret)
	}

	return nil
}
