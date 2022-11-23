package dqueue

import (
	//    "fmt"
	"context"
	"errors"
	"strings"

	"github.com/go-redis/redis/v8"
	//    "github.com/go-zookeeper/zk"

	"time"
)

/*
 * Можно создавать несколько очередей
 *
 * Для клиента они различаются именами
 *
 * В реализации они могут потребовать вспомогательных данных
 * Для них - эта структура. Можете определить в ней любые поля
 */
type DQueue struct {
	name                string
	nShards             int
	currentShardForPush int
	shards              []*redis.Client
}

var all_shards []*redis.Client
var queues_info map[string]int = make(map[string]int)
var ctx = context.Background()
var time_layout = "2006-01-02 15:04:05.999999999 -0700 MST"

/*
 * Запомнить данные и везде использовать
 */
func Config(redisOptions *[]*redis.Options, zkCluster []string) {
	all_shards = []*redis.Client{}
	for _, opts := range *redisOptions {
		all_shards = append(all_shards, redis.NewClient(opts))
	}
}

/*
 * Открываем очередь на nShards шардах
 *
 * Попытка создать очередь с существующим именем и другим количеством шардов
 * должна приводить к ошибке
 *
 * При попытке создать очередь с существующим именем и тем же количеством шардов
 * нужно вернуть экземпляр DQueue, позволяющий делать Push/Pull
 *
 * Предыдущее открытие может быть совершено другим клиентом, соединенным с любым узлом
 * Redis-кластера
 *
 * Отдельные узлы Redis-кластера могут выпадать. Availability очереди в целом
 * не должна от этого страдать
 *
 */
func Open(name string, nShards int) (DQueue, error) {
	if nShards > len(all_shards) {
		return DQueue{name, 0, 0, []*redis.Client{}}, errors.New("error: not enough shards")
	}
	n_shards_in_storage := queues_info[name]

	if n_shards_in_storage == 0 || n_shards_in_storage == nShards {
		return DQueue{name, nShards, 0, all_shards[:nShards]}, nil
	} else {
		return DQueue{name, 0, 0, []*redis.Client{}}, errors.New("error: queue with this name already exists")
	}

}

/*
 * Пишем в очередь. Каждый следующий Push - в следующий шард
 *
 * Если шард упал - пропускаем шард, пишем в следующий по очереди
 */
func (q *DQueue) Push(value string) error {
	for i := 0; i < q.nShards; i++ {
		var shard = q.shards[q.currentShardForPush]
		var value_time = value + "::" + time.Now().Format(time_layout)
		err := shard.LPush(ctx, q.name, value_time).Err()
		q.currentShardForPush = (q.currentShardForPush + 1) % q.nShards
		if err == nil || err == redis.Nil {
			return nil
		}
	}
	return errors.New("error: push value failed")
}

/*
 * Читаем из очереди
 *
 * Из того шарда, в котором самое раннее сообщение
 *
 */
func (q *DQueue) Pull() (string, error) {
	var min_time time.Time
	var is_min_time_init = false
	var res string = ""
	var has_value = false
	for i := 0; i < q.nShards; i++ {
		var shard = q.shards[i]
		str_val, err := shard.RPop(ctx, q.name).Result()
		if err != nil && err != redis.Nil {
			continue
		}
		if str_val == "" {
			continue
		}

		val_and_time := strings.Split(str_val, "::")
		var val = val_and_time[0]
		push_time, err := time.Parse(time_layout, val_and_time[1])
		if err != nil {
			continue
		}
		if !is_min_time_init || push_time.Before(min_time) {
			min_time = push_time
			is_min_time_init = true
			res = val
			has_value = true
		}

	}
	if has_value {
		return res, nil
	} else {
		return "", errors.New("error: pull value failed")
	}
}
