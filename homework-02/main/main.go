package main

import (
	"fmt"

	//"context"

	"github.com/go-redis/redis/v8"

	//"github.com/go-zookeeper/zk"

	"dqueue"

	"os"
)

func main() {
	argsWithoutProg := os.Args[1:]
	if len(argsWithoutProg) != 4 {
		fmt.Println("error: wrong number of args - expected 4 addresses of redis shards")
		return
	}
	redisOptions1 := redis.Options{
		Addr:     argsWithoutProg[0],
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	redisOptions2 := redis.Options{
		Addr:     argsWithoutProg[1],
		Password: "", // no password set
		DB:       0,  // use default DB
	}
	redisOptions3 := redis.Options{
		Addr:     argsWithoutProg[2],
		Password: "", // no password set
		DB:       0,  // use default DB
	}
	redisOptions4 := redis.Options{
		Addr:     argsWithoutProg[3],
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	var redisOptions = []*redis.Options{&redisOptions1, &redisOptions2, &redisOptions3, &redisOptions4}

	dqueue.Config(&redisOptions, []string{"127.0.0.1"})
	_, err := dqueue.Open("alex-greben:queue1", 4)
	if err != nil {
		fmt.Println(err.Error())
	}

}
