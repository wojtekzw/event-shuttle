package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"log"
	"math"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var EVENTS_BUCKET = []byte("events")

type Store struct {
	db               *bolt.DB
	eventsIn         chan *EventIn
	eventsOut        chan *EventOut
	eventsDelivered  chan int64
	eventsFailed     chan int64
	readPointer      int64
	readPointerLock  sync.RWMutex
	readFromStore    int64
	writePointer     int64
	writePointerLock sync.RWMutex
	writtenToStore   int64
	readTrigger      chan bool
	stopStore        chan bool
	stopRead         chan bool
	stopClean        chan bool
	stopReport       chan bool
	stopSync         chan bool
	shutdown         chan bool
}

func (s *Store) EventsInChannel() chan<- *EventIn {
	return s.eventsIn
}

func (s *Store) EventsOutChannel() <-chan *EventOut {
	return s.eventsOut
}

func (s *Store) syncDB() error {
	return s.db.Sync()
}

func (s *Store) syncStore() {
	for {
		select {
		case <-s.stopSync:
			log.Println("go=sync at=shtudown-sync")
			s.shutdown <- true
			return
		case _, ok := <-time.After(10 * time.Second):
			if ok {
				err := s.syncDB()
				if err != nil {
					log.Printf("go=sync at=sync-interval db sync error=%v ", err)
				} else {
					log.Printf("go=sync at=sync-interval db sync OK ")

				}

			}
		}

	}
}

func OpenStore(dbFile string) (*Store, error) {
	log.Printf("go=open at=open-db\n")
	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	// Do not sync every insert
	// Do sync every 10 seconds
	db.NoSync = true

	if err != nil {
		log.Printf("go=open at=error-openiing-db error=%s\n", err)
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, berr := tx.CreateBucketIfNotExists(EVENTS_BUCKET)
		return berr
	})

	if err != nil {
		log.Printf("go=open at=error-creating-bucket error=%s\n", err)
		return nil, err
	}

	gob.Register(Event{})

	lastWritePointer, readPointer, err := findReadAndWritePointers(db)
	if err != nil {
		log.Printf("go=open at=read-pointers-error error=%s\n", err)
		return nil, err
	}
	log.Printf("go=open at=read-pointers read=%d write=%d\n", readPointer, lastWritePointer+1)

	store := &Store{
		db:              db,
		eventsIn:        make(chan *EventIn),
		eventsOut:       make(chan *EventOut, 32),
		eventsDelivered: make(chan int64, 32),
		eventsFailed:    make(chan int64, 32),
		writePointer:    lastWritePointer + 1,
		readPointer:     readPointer,
		readTrigger:     make(chan bool, 1), //buffered so reader can send to itself
		stopStore:       make(chan bool, 1),
		stopRead:        make(chan bool, 1),
		stopClean:       make(chan bool, 1),
		stopReport:      make(chan bool, 1),
		stopSync:        make(chan bool, 1),
		shutdown:        make(chan bool, 5),
	}

	go store.readEvents()
	go store.cleanEvents()
	go store.storeEvents()
	go store.report()
	go store.syncStore()

	store.readTrigger <- true
	log.Printf("go=open at=store-created")
	return store, nil
}

func (s *Store) Close() error {
	s.stopStore <- true
	s.stopClean <- true
	s.stopRead <- true
	s.stopReport <- true
	s.stopSync <- true
	//drain events out so the readEvents goroutine can unblock and exit.
	s.drainEventsOut()
	//close channels so receivers can unblock and exit
	//external senders should wrap sends in a recover so they dont panic
	//when the channels are closed on shutdown
	close(s.eventsIn)
	close(s.eventsDelivered)
	close(s.eventsFailed)
	<-s.shutdown
	<-s.shutdown
	<-s.shutdown
	<-s.shutdown
	<-s.shutdown
	return s.db.Close()
}

// store events runs in a goroutine and receives from s.eventsIn and saves events to bolt
func (s *Store) storeEvents() {
	for {
		select {
		case e, ok := <-s.eventsIn:
			if ok {
				err := s.writeEvent(s.getWritePointer(), e)
				if err == nil {
					s.incrementWritePointer()
					s.triggerRead()
				} else {
					log.Printf("go=store at=write-error error=%s", err)
				}
				e.saved <- err == nil
			}
		case <-s.stopStore:
			log.Println("go=store at=shtudown-store")
			s.shutdown <- true
			return
		}

	}
	log.Println("go=store at=exit")

}

// readEvents runs in a goroutine and reads events from boltdb and sends them on s.eventsOut
// it also receives from the events reRead channel and resends on s.eventsOut
// it is the owning sender for s.eventsOut, so it closes the channel on shutdown.
func (s *Store) readEvents() {
	for {
		select {
		case <-s.stopRead:
			log.Println("go=read at=shutdown-read")
			close(s.eventsOut)
			s.shutdown <- true
			return
		case _, ok := <-s.readTrigger:
			if ok {
				read := s.getReadPointer()
				event, err := s.readEvent(read)
				if event != nil {
					s.eventsOut <- event
					s.incrementReadPointer(read)
					s.triggerRead()
				}
				if err == nil && event == nil && s.getWritePointer() > read {
					s.incrementReadPointer(read)
					s.triggerRead()
				}

				if err != nil {
					log.Printf("go=read at=read-error error=%s", err)
				}
			}
		}

	}
	log.Println("go=read at=exit")
}

// cleanEvents runs in a goroutine and  recieves from s.eventsDelivered and s.eventsFailed.
// for delivered events it removes the event from bolt
// for failed events it reReads the event and sends it on
// owning sender for s.eventsReRead so it closes it on shutdown
func (s *Store) cleanEvents() {
	for {
		select {
		case <-s.stopClean:
			log.Println("go=clean at=shutdown-clean")
			s.shutdown <- true
			return
		case delivered, ok := <-s.eventsDelivered:
			if ok {
				if delivered%1000 == 0 {
					log.Printf("go=clean at=delete sequence=%d", delivered)
				}
				s.deleteEvent(delivered)
			}
		case failed, ok := <-s.eventsFailed:
			if ok {
				if s.getReadPointer() > failed {
					s.setReadPointer(failed)
				}
			}
		}
	}

	log.Println("go=clean at=exit")
}

func (s *Store) report() {
	for {
		select {
		case <-s.stopReport:
			log.Println("go=report at=shutdown-report")
			read := s.getReadPointer()
			write := s.getWritePointer()
			log.Printf("go=report at=shutdown-report read=%d write=%d delta=%d", read, write, write-read)
			log.Printf("go=report at=shutdown-report readFromStore=%d writtenToStore=%d delta=%d", s.readFromStore, s.writtenToStore, s.writtenToStore-s.readFromStore)
			s.shutdown <- true
			return
		case _, ok := <-time.After(10 * time.Second):
			if ok {
				read := s.getReadPointer()
				write := s.getWritePointer()
				log.Printf("go=report at=report read=%d write=%d delta=%d", read, write, write-read)
				log.Printf("go=report at=report readFromStore=%d writtenToStore=%d delta=%d", s.readFromStore, s.writtenToStore, s.writtenToStore-s.readFromStore)

			}
		}

	}

	log.Println("go=clean at=exit")
}

func (s *Store) writeEvent(seq int64, e *EventIn) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(EVENTS_BUCKET)
		encoded, err := encodeEvent(e.event)
		if err != nil {
			log.Printf("go=store at=encode-fail error=%s\n", err)
			return err
		}
		err = bucket.Put(writeSequence(seq), encoded)
		if err != nil {
			log.Printf("go=store at=put-fail error=%s\n", err)
			return err
		}
		_ = s.incrementWrittenToStore()
		if seq%1000 == 0 {
			log.Printf("go=store at=wrote sequence=%d", seq)
		}
		return nil

	})
	return err
}

// reRead event reads an event that was reported to have failed to be delivered and reSends it on s.eventsReRead
func (s *Store) readEvent(seq int64) (*EventOut, error) {
	var eventOut *EventOut
	err := s.db.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket(EVENTS_BUCKET)
		eventBytes := events.Get(writeSequence(seq))
		if eventBytes == nil || len(eventBytes) == 0 {
			return nil
		} else {
			_ = s.incrementReadFromStore()
			if seq%1000 == 0 {
				log.Printf("go=read at=read sequence=%d", seq)
			}
			event, err := decodeEvent(eventBytes)
			if err != nil {
				log.Printf("go=read at=decode-fail error=%s\n", err)
				return err
			}
			eventOut = &EventOut{sequence: seq, event: event}
			return nil
		}
	})
	return eventOut, err
}

func (s *Store) deleteEvent(seq int64) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket(EVENTS_BUCKET)
		err := events.Delete(writeSequence(seq))
		return err

	})
	if err != nil {
		log.Printf("go=delete at=delete-error error=%s", err)
	}
	return err
}

func (s *Store) triggerRead() {
	select {
	case s.readTrigger <- true:
	default:
	}
}

func (s *Store) drainEventsOut() {
	for {
		_, ok := <-s.eventsOut
		if !ok {
			return
		}
	}
}

func (s *Store) incrementReadPointer(read int64) {
	s.readPointerLock.Lock()
	defer s.readPointerLock.Unlock()
	if s.readPointer == read {
		s.readPointer += 1
	}
}

func (s *Store) setReadPointer(rp int64) {
	s.readPointerLock.Lock()
	defer s.readPointerLock.Unlock()
	s.readPointer = rp
}

func (s *Store) incrementWritePointer() {
	s.writePointerLock.Lock()
	defer s.writePointerLock.Unlock()
	s.writePointer += 1
}

func (s *Store) getReadPointer() int64 {
	s.readPointerLock.RLock()
	defer s.readPointerLock.RUnlock()
	return s.readPointer
}

func (s *Store) getWritePointer() int64 {
	s.writePointerLock.RLock()
	defer s.writePointerLock.RUnlock()
	return s.writePointer
}

func findReadAndWritePointers(db *bolt.DB) (int64, int64, error) {
	writePointer := int64(0)
	readPointer := int64(math.MaxInt64)
	err := db.View(func(tx *bolt.Tx) error {
		events := tx.Bucket(EVENTS_BUCKET)
		err := events.ForEach(func(k, v []byte) error {
			seq, _ := readSequence(k)
			if seq > writePointer {
				writePointer = seq
			}
			if seq < readPointer {
				readPointer = seq
			}
			return nil
		})

		return err
	})

	if err != nil {
		return -1, -1, err
	}

	if readPointer == int64(math.MaxInt64) {
		readPointer = int64(1)
	}

	return writePointer, readPointer, nil
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

func (s *Store) incrementWrittenToStore() int64 {
	s.writtenToStore++
	return s.writtenToStore
}

func (s *Store) incrementReadFromStore() int64 {
	s.readFromStore++
	return s.readFromStore
}
