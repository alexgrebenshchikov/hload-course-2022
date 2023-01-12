package local_redis

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type RedisCluster struct {
	RedisOptions redis.Options
	Ctx          context.Context
	Prefix       string
	Master       string
	Workers      []string
}

var rdscl *redis.Client
var rds_prefix string
var rds_ctx = context.Background()

func SetupRedisClient(opts *redis.Options, prefix string) {
	rdscl = redis.NewClient(opts)
	rds_prefix = prefix
}

func SetShortUrl(tinyUrl string, longUrl string) bool {
	err := rdscl.Set(rds_ctx, rds_prefix+"_"+tinyUrl, longUrl, 0).Err()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	return true
}

func IncreaseClicksCount(tinyUrl string) int64 {
	result, err := rdscl.Incr(rds_ctx, rds_prefix+"_"+tinyUrl+"_"+"clicks").Result()
	if err != nil {
		fmt.Println(err.Error())
		return -1
	}
	return result
}

func CheckTinyUrl(tinyUrl string) (string, int64, error) {

	longUrl, err := rdscl.Get(rds_ctx, rds_prefix+"_"+tinyUrl).Result()
	if err != nil {
		return "", -1, err
	}

	clicks := IncreaseClicksCount(tinyUrl)
	return longUrl, clicks, nil
}
