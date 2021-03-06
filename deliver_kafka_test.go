package main

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	log "github.com/Sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

const localKafka = DefaultKafkaBrokers

func init() {
	initLog(log.DebugLevel)
}

func kafkaIsUp() bool {
	conn, err := net.Dial("tcp", localKafka)

	if err != nil {
		log.Errorf("Kafka does not appear to be up on %s, skipping this test", localKafka)
		return false
	}

	conn.Close()
	return true

}

func TestKafkaConfig(t *testing.T) {
	if kafkaIsUp() {
		d, err := NewKafkaDeliver(nil, "testClientId", []string{localKafka})
		assert.Nil(t, err, fmt.Sprintf("%v+", err))
		for i := 0; i < 10000; i++ {
			// err = d.producer.SendMessage("test", nil, sarama.StringEncoder("hello world"))
			msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("hello world")}
			partition, offset, err := d.producer.SendMessage(msg)
			assert.Nil(t, err, fmt.Sprintf("Partition:%d, Offset:%d, Error:%v+", partition, offset, err))
		}
	}
}

func TestNewKafkaDeliver(t *testing.T) {
	if kafkaIsUp() {
		store, err := OpenStore("test.db")
		assert.Nil(t, err, err)
		go func() {
			log.Debugf("Error creating http server %v\n", http.ListenAndServe("localhost:6060", nil))
		}()
		d, err := NewKafkaDeliver(store, "testClientId", []string{localKafka})
		d.Start()
		ack := make(chan bool)
		d.store.eventsInChannel() <- &EventIn{saved: ack, event: &Event{Channel: "test", Body: []byte("{}")}}
		acked := <-ack
		assert.True(t, acked, "not acked")
		time.Sleep(time.Second * 5)
		d.Stop()
		d.Store().Close()
	}
}

func BenchmarkKafkaProduceAndSend(b *testing.B) {
	if kafkaIsUp() {
		d, err := NewKafkaDeliver(nil, "testClientId", []string{localKafka})
		assert.Nil(b, err, fmt.Sprintf("%v+", err))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msg := &sarama.ProducerMessage{Topic: "test", Value: sarama.StringEncoder("hello BenchmarkKafkaProduceAndSend")}
			partition, offset, err := d.producer.SendMessage(msg)
			if err != nil {
				assert.Nil(b, err, fmt.Sprintf("Partition:%d, Offset:%d, Error:%v+", partition, offset, err))
			}
		}
		b.StartTimer()
	}
}
