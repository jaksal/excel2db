package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// RedisConf redis config for reload
type RedisConf struct {
	Addr string `json:"addr"`
	Db   int    `json:"db"`
}

// ServerConf update db, redis config
type ServerConf struct {
	Db     []string   `json:"db"`
	Redis  *RedisConf `json:"redis"`
	Server string     `json:"server"`
}

// ServerList server conf list
type ServerList map[string]*ServerConf

// ReadServerConf read server config list
func ReadServerConf(path string, tag string) (*ServerConf, error) {
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("invalid confing path=%s err=%s", path, err)
	}

	var result ServerList
	if err := json.Unmarshal(dat, &result); err != nil {
		return nil, fmt.Errorf("config parse error path=%s err=%s", path, err)
	}

	if s, exist := result[tag]; exist {
		return s, nil
	}

	return nil, fmt.Errorf("not found server:%s", tag)
}
