package main

import (
	"github.com/Shopify/sarama"

	log "github.com/Sirupsen/logrus"
)

var maxDeliverGoroutines int = 8

type Deliver interface {
	Store() *Store
	Start() error
	Stop() error
}

type KafkaDeliver struct {
	store             *Store
	clientId          string
	brokerList        []string
	config            *sarama.Config
	client            sarama.Client
	producer          sarama.SyncProducer
	deliverGoroutines int
	shutdownDeliver   chan bool
	shutdown          chan bool
}

func NewKafkaDeliver(store *Store, clientId string, brokerList []string) (*KafkaDeliver, error) {
	log.Debugf("go=kafka at=new-kafka-deliver\n")

	config := sarama.NewConfig()

	config.ClientID = clientId
	config.Producer.RequiredAcks = sarama.WaitForAll

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		return nil, err
	}
	log.Debugf("go=kafka at=created-client\n")

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	log.Debugf("go=kafka at=created-producer\n")

	defer func() {
		if err != nil {
			log.Errorf("go=kafka at=defer-close-producer after error %v\n", err)
			if err := producer.Close(); err != nil {
				log.Fatalf("go=kafka at=producer-close fatal error %v\n", err)
			}
		}
	}()

	return &KafkaDeliver{
		clientId:          clientId,
		brokerList:        brokerList,
		store:             store,
		producer:          producer,
		client:            client,
		config:            config,
		deliverGoroutines: maxDeliverGoroutines,
		shutdownDeliver:   make(chan bool, maxDeliverGoroutines),
		shutdown:          make(chan bool, maxDeliverGoroutines),
	}, nil

}

func (k *KafkaDeliver) Store() *Store {
	return k.store
}

func (k *KafkaDeliver) Start() error {
	for i := 0; i < k.deliverGoroutines; i++ {
		go k.deliverEvents(i)
	}
	return nil
}

func (k *KafkaDeliver) deliverEvents(num int) {
	for {
		select {
		case <-k.shutdownDeliver:
			k.shutdown <- true
			return
		case event, ok := <-k.store.eventsOut:
			if ok {
				msg := &sarama.ProducerMessage{Topic: event.event.Channel, Value: sarama.ByteEncoder(event.event.Body)}
				partition, offset, err := k.producer.SendMessage(msg)

				if err != nil {
					log.Errorf("go=deliver num=%d at=send-error error=%v,partition=%d, offset=%d", num, err, partition, offset)
					noAckEvent(k.store, event.sequence)
				} else {
					ackEvent(k.store, event.sequence)
				}
			}
		}
	}
}

func ackEvent(store *Store, seq int64) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("at=recover-ack-panic\n")
		}
	}()
	// the store owns the ack channel and can close it on shutdown
	// so we wrap this call which can panic in a recover
	ack(store, seq)
}

func noAckEvent(store *Store, seq int64) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("at=recover-noack-panic\n")
		}
	}()
	// the store owns the noAck channel and can close it on shutdown
	// so we wrap this call which can panic in a recover
	noAck(store, seq)
}

func ack(store *Store, seq int64) {
	store.eventsDelivered <- seq
}

func noAck(store *Store, seq int64) {
	store.eventsFailed <- seq
}

func (k *KafkaDeliver) Stop() error {
	for i := 0; i < k.deliverGoroutines; i++ {
		k.shutdownDeliver <- true
	}
	for i := 0; i < k.deliverGoroutines; i++ {
		<-k.shutdown
	}
	return nil
}
