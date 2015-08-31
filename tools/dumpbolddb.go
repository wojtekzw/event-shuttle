package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/boltdb/bolt"
)

type Event struct {
	Channel string
	Body    []byte
}

func readSequence(seq []byte) (int64, error) {
	return binary.ReadVarint(bytes.NewBuffer(seq))
}

func writeSequence(seq int64) []byte {
	buffer := make([]byte, 16)
	binary.PutVarint(buffer, seq)
	return buffer
}

func decodeEvent(eventBytes []byte) (*Event, error) {
	evt := &Event{}
	err := gob.NewDecoder(bytes.NewBuffer(eventBytes)).Decode(evt)
	return evt, err
}

func encodeEvent(evt *Event) ([]byte, error) {
	eventBytes := new(bytes.Buffer)
	err := gob.NewEncoder(eventBytes).Encode(evt)
	return eventBytes.Bytes(), err
}

func DumpStore() {

	dbName := "events.db"

	db, err := bolt.Open(dbName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Panicf("unable to open %s, error: %v\n", dbName, err)
	}
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("events"))
		b.ForEach(func(k, v []byte) error {

			seq, err := readSequence(k)
			if err != nil {
				log.Errorf("Error decoding sequence number %v", err)
			}

			evt, err := decodeEvent(v)
			if err != nil {
				log.Errorf("Error decoding event %v", err)
			}
			body := strings.Replace(string(evt.Body), "\n", "\\n", -1)
			fmt.Printf("key=%d, value={channel:\"%s\", body:\"%s\"}\n", seq, evt.Channel, body)
			return nil
		})
		return nil
	})
}

func main() {
	DumpStore()
}
