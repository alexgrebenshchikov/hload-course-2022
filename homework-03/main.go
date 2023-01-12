package main

import (
	_ "github.com/lib/pq"

	kf "main/kafka"

	dbi "main/db_interactor"

	srv "main/server_api"

	"fmt"

	"os"
)

func main() {
	argsWithoutProg := os.Args[1:]
	if len(argsWithoutProg) != 1 {
		fmt.Println("error: wrong number of args - expected server address")
		return
	}
	var db = dbi.OpenSQLConnection()
	db.CreateTableIfNotExists()
	db.CreateClicksCountTableIfNotExists()
	urlWriter := kf.CreateUrlWriter()
	clickReader := kf.CreateClickReader()
	go kf.ClickConsume(clickReader, &db)
	router := srv.SetupMasterServerRouter(&db, urlWriter)

	router.Run(argsWithoutProg[0])
}
