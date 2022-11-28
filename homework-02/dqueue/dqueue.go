package dqueue

import (
	//    "fmt"
	"context"
	"errors"
	"fmt"
	"strings"

	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-zookeeper/zk"
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

type ZKQueueNodeData struct {
	nShards   int
	curIdPush int
	version   int32
}

var all_shards []*redis.Client
var ctx = context.Background()
var time_layout = "2006-01-02 15:04:05.999999999 -0700 MST"
var zkClusterAddrs = []string{}
var zkHead = "/zookeeper/alex-greben0000000003"
var zkSeqNodeNum = 10

/*
 * Запомнить данные и везде использовать
 */
func Config(redisOptions *[]*redis.Options, zkCluster []string) {
	all_shards = []*redis.Client{}
	for _, opts := range *redisOptions {
		all_shards = append(all_shards, redis.NewClient(opts))
	}
	zkClusterAddrs = zkCluster
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
	c, _, err := zk.Connect(zkClusterAddrs, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	err = Lock(c, zkHead)
	if err != nil {
		return DQueue{}, err
	}

	queuePath := zkHead + "/" + name
	ex, _, err := c.Exists(queuePath)
	if err != nil {
		return DQueue{}, err
	}
	if ex {
		s, st, err := c.Get(queuePath)
		if err != nil {
			return DQueue{}, err
		}

		nodeData, err := parseQueueNodeData(string(s), st)
		if err != nil {
			return DQueue{}, err
		}
		if nodeData.nShards != nShards {
			return DQueue{}, fmt.Errorf("conflict nshards with existing queue")
		}
		return DQueue{name, nShards, 0, all_shards[:nShards]}, nil
	}

	_, err = c.Create(queuePath, []byte(fmt.Sprintf("%d|%d", nShards, 0)), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return DQueue{}, err
	}
	return DQueue{name, nShards, 0, all_shards[:nShards]}, nil
}

func parseQueueNodeData(data string, st *zk.Stat) (ZKQueueNodeData, error) {
	s := strings.Split(data, "|")
	nShards, err := strconv.Atoi(s[0])
	if err != nil {
		return ZKQueueNodeData{}, err
	}
	curIdPush, err := strconv.Atoi(s[1])
	if err != nil {
		return ZKQueueNodeData{}, nil
	}
	return ZKQueueNodeData{curIdPush: curIdPush, nShards: nShards, version: st.Version}, nil
}

func pushQueueNodeData(data ZKQueueNodeData, c *zk.Conn, queuePath string) error {
	_, err := c.Set(queuePath, []byte(fmt.Sprintf("%d|%d", data.nShards, data.curIdPush)), data.version)
	if err != nil {
		return err
	}

	return nil
}

/*
 * Пишем в очередь. Каждый следующий Push - в следующий шард
 *
 * Если шард упал - пропускаем шард, пишем в следующий по очереди
 */
func (q *DQueue) Push(value string) error {
	c, _, err := zk.Connect(zkClusterAddrs, time.Second*10)
	if err != nil {
		return err
	}
	defer c.Close()

	queuePath := zkHead + "/" + q.name
	err = Lock(c, queuePath)
	if err != nil {
		return err
	}

	s, st, err := c.Get(queuePath)
	if err != nil {
		return err
	}

	nodeData, err := parseQueueNodeData(string(s), st)
	if err != nil {
		return err
	}
	for i := 0; i < q.nShards; i++ {
		var shard = q.shards[nodeData.curIdPush]
		var cur_time = time.Now()
		var value_time = value + "::" + cur_time.Format(time_layout)
		err := shard.RPush(ctx, q.name, value_time).Err()
		nodeData.curIdPush = (nodeData.curIdPush + 1) % nodeData.nShards
		if err == nil || err == redis.Nil {
			err = pushQueueNodeData(nodeData, c, queuePath)
			if err != nil {
				fmt.Println(err.Error())
			}
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
	var res_shard_index = 0
	for i := 0; i < q.nShards; i++ {
		var shard = q.shards[i]
		str_val, err := shard.LRange(ctx, q.name, 0, 0).Result()
		if err != nil && err != redis.Nil {
			continue
		}
		if len(str_val) == 0 {
			continue
		}
		val_and_time := strings.Split(str_val[0], "::")
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
			res_shard_index = i
		}

	}
	if has_value {
		_, err := q.shards[res_shard_index].LPop(ctx, q.name).Result()
		if err != nil && err != redis.Nil {
			fmt.Println(err.Error())
		}
		return res, nil
	} else {
		return "", errors.New("error: pull value failed")
	}
}

func Lock(c *zk.Conn, path string) error {
	locknodePath := path + "/_locknode"
	ex, _, err := c.Exists(locknodePath)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	if !ex {
		act_path, err := c.Create(locknodePath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		fmt.Println(act_path)
	}
	path, err = c.Create(locknodePath+"/lock", []byte{}, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll)) // TODO or `+`?
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	seqNum, err := getNumFromSeqNodePath(path)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	for {
		children, _, err := c.Children(locknodePath)
		if err != nil {
			return err
		}
		var minNode = seqNum
		var minId = -1
		for i, child := range children {
			nodeNum, _ := getNumFromSeqNodePath(child)
			if nodeNum < minNode {
				minNode = nodeNum
				minId = i
			}
		}

		if minNode < seqNum {
			e, _, ch, err := c.ExistsW(children[minId])
			if err != nil {
				return err
			}

			if !e {
				continue
			}

			_ = <-ch
			continue
		} else {
			return nil
		}
	}
}

func getNumFromSeqNodePath(path string) (int, error) {
	i, err := strconv.Atoi(path[len(path)-zkSeqNodeNum:])
	if err != nil {
		return -1, err
	}

	return i, nil
}
