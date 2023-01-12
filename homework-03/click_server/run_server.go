package main

import (
	"os"

	"fmt"

	_ "github.com/lib/pq"

	dbi "main/db_interactor"

	srv "main/server_api"

	kf "main/kafka"

	"github.com/go-redis/redis/v8"

	lr "main/local_redis"
)

func main() {
	argsWithoutProg := os.Args[1:]
	if len(argsWithoutProg) != 3 {
		fmt.Println("error: wrong number of args - expected server address, redis address and worker id")
		return
	}
	var db = dbi.OpenSQLConnection()
	db.CreateTableIfNotExists()
	reader := kf.CreateUrlReader(argsWithoutProg[2])
	writer := kf.CreateClickWriter()
	go kf.UrlConsume(reader)
	redisOptions := redis.Options{
		Addr:     argsWithoutProg[1],
		Password: "", // no password set
		DB:       0,  // use default DB
	}
	lr.SetupRedisClient(&redisOptions, "alex-greben")
	router := srv.SetupClickServerRouter(&db, reader, writer)

	router.Run(argsWithoutProg[0])
}
