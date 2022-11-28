package main

import (
	"fmt"

	//"context"

	//"time"

	"github.com/go-redis/redis/v8"
	//"github.com/go-zookeeper/zk"

	"dqueue"
	"os"
)

func main() {
	argsWithoutProg := os.Args[1:]
	if len(argsWithoutProg) != 7 {
		fmt.Println("error: wrong number of args - expected 4 addresses of redis shards and 3 addresses of zk cluster")
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
	var zkCluster = []string{argsWithoutProg[4], argsWithoutProg[5], argsWithoutProg[6]}

	dqueue.Config(&redisOptions, zkCluster)
	dq, err := dqueue.Open("alex-greben-queue2", 4)
	if err != nil {
		panic(err)
	}
	err = dq.Push("1")
	if err != nil {
		panic(err)
	}
	val, err := dq.Pull()
	if err != nil {
		panic(err)
	}
	fmt.Println(val)
}
