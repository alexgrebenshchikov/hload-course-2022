package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/segmentio/kafka-go"

	dbi "main/db_interactor"
	lr "main/local_redis"
)

const (
	urlTopic       = "alex-greben-urls"
	clickTopic     = "alex-greben-clicks-count"
	broker1Address = "158.160.19.212:9092"
	clickIncr      = 100
)

var kf_ctx = context.Background()

func CreateUrlWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{broker1Address},
		Topic:        urlTopic,
		RequiredAcks: 1,
	})
}

func CreateClickWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{broker1Address},
		Topic:        clickTopic,
		RequiredAcks: 1,
	})

}

func CreateUrlReader(worked_id string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		GroupID: "alex-greben-urls:" + worked_id,
		Brokers: []string{broker1Address},
		Topic:   urlTopic,
	})
}

func CreateClickReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		GroupID: "alex-greben-clicks",
		Brokers: []string{broker1Address},
		Topic:   clickTopic,
	})
}

func UrlProduce(writer *kafka.Writer, longUrl string, tinyUrl string) {
	err := writer.WriteMessages(kf_ctx, kafka.Message{
		Key:   []byte(tinyUrl),
		Value: []byte(longUrl),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}
	fmt.Println("writes:", tinyUrl+":"+longUrl)

}

func ClickProduce(writer *kafka.Writer, shorturl string, clicks int64) {
	err := writer.WriteMessages(kf_ctx, kafka.Message{
		Key:   []byte(shorturl),
		Value: []byte(strconv.FormatInt(clicks, 10)),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}
	fmt.Println("writes:", clicks)

}

func UrlConsume(reader *kafka.Reader) {
	for {
		msg, err := reader.FetchMessage(kf_ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		lr.SetShortUrl(string(msg.Key), string(msg.Value))
		fmt.Println("received: ", string(msg.Key)+":"+string(msg.Value))
		reader.CommitMessages(kf_ctx, msg)
	}
}

func ClickConsume(reader *kafka.Reader, db *dbi.DbInteractor) {
	for {
		msg, err := reader.FetchMessage(kf_ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		cnt, err := strconv.Atoi(string(msg.Value))
		if err != nil {
			fmt.Println("counter parsing error")
		}
		db.UpdateCounter(string(msg.Key), cnt)
		fmt.Println("received: ", string(msg.Key)+":"+string(msg.Value))
		reader.CommitMessages(kf_ctx, msg)
	}
}
