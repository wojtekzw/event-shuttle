package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/boltdb/bolt"
)

// Event - main event type
type Event struct {
	Channel string
	Body    []byte
}

// EventIn - struct for channel
type EventIn struct {
	event *Event
	saved chan bool
}

// EventOut - struct for channel
type EventOut struct {
	event    *Event
	sequence int64
}

var defaultEventBucket = []byte("events")

// Store is main tyoe for storing events via channels
type Store struct {
	db               *bolt.DB
	eventsIn         chan *EventIn  // full event to store in BoltDB goes into this channel
	eventsOut        chan *EventOut // full event read from BoltDB and to be delivered to outside system
	eventsDelivered  chan int64     // sequence number (key) of succesfully delivered event goes into this channel
	eventsFailed     chan int64     // sequence number (key) of failed event (not delivered to outside system) goes into this channel
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
	stopReconnect    chan bool
	shutdown         chan bool
}

func (s *Store) eventsInChannel() chan<- *EventIn {
	return s.eventsIn
}

func (s *Store) eventsOutChannel() <-chan *EventOut {
	return s.eventsOut
}

func (s *Store) syncDB() error {
	return s.db.Sync()
}

// syncStore runs in a goroutine and and makes BoltDB sync every 10 seconds if it was opened with
// noSync=true option for better preformance and greater risk
// It waits for signal on s.stopSync channel to stop itself
func (s *Store) syncStore() {
	for {
		select {
		case <-s.stopSync:
			s.shutdown <- true
			return
		case _, ok := <-time.After(10 * time.Second):
			if ok {
				err := s.syncDB()
				if err != nil {
					log.Errorf("go=sync at=sync-interval db sync error=%v ", err)
				}

			}
		}

	}
}

// OpenStore opens BoildDB database creating database and bucket if not exists
// sets writePointer and readPointer from Bolt - to enable sending out events that still sit in database
func OpenStore(dbFile string) (*Store, error) {

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})

	if err != nil {
		log.Errorf("go=open at=error-opening-db (%s) error=%s\n", dbFile, err)
		return nil, err
	}
	// Do not sync every insert
	// Do sync every 10 seconds
	db.NoSync = true

	err = db.Update(func(tx *bolt.Tx) error {
		_, berr := tx.CreateBucketIfNotExists(defaultEventBucket)
		return berr
	})

	if err != nil {
		log.Errorf("go=open at=error-creating-bucket error=%s\n", err)
		return nil, err
	}
	// Register event type to be encoded and decoded
	gob.Register(Event{})

	// lastWritePointer - the last sequence number of event stored in BoldDB -  we should write to BoldDB with NEXT number
	// writePointer = lastWritePointer +1
	// readPointer - lowest sequence nmber of event in BoldDB - we should read from BoldDB from THIS number
	lastWritePointer, readPointer, err := findReadAndWritePointers(db)
	if err != nil {
		log.Errorf("go=open at=read-pointers-error error=%s\n", err)
		return nil, err
	}

	// increment writePointer to point to new value
	// Fatal error if wrtePointe is MaxInit64 or < 0
	if lastWritePointer == math.MaxInt64 {
		log.Errorf("go=open error Max number of lastWritePointer: %d", lastWritePointer)
		os.Exit(4)
	}

	if lastWritePointer < 0 {
		log.Errorf("go=open error invalid value of lastWritePointer: %d", lastWritePointer)
		os.Exit(4)
	}

	writePointer := lastWritePointer + 1

	if writePointer < readPointer {
		log.Errorf("go=open error lastWritePointer:%d < readPointer:%d", writePointer, readPointer)
		os.Exit(4)
	}

	// if writePointer > readPointer {
	// 	log.Infof("go=open error recovery: lastWritePointer:%d < readPointer:%d, delta:%d - will send to Kafka now", writePointer, readPointer, writePointer-readPointer)
	// }

	store := &Store{
		db:              db,
		eventsIn:        make(chan *EventIn),
		eventsOut:       make(chan *EventOut, 32),
		eventsDelivered: make(chan int64, 32),
		eventsFailed:    make(chan int64, 32),
		writePointer:    writePointer,
		readPointer:     readPointer,
		readTrigger:     make(chan bool, 1), //buffered so reader can send to itself
		stopStore:       make(chan bool, 1),
		stopRead:        make(chan bool, 1),
		stopClean:       make(chan bool, 1),
		stopReport:      make(chan bool, 1),
		stopSync:        make(chan bool, 1),
		stopReconnect:   make(chan bool, 1),
		shutdown:        make(chan bool, 6),
	}

	go store.readEvents()
	go store.cleanEvents()
	go store.storeEvents()
	go store.report()
	go store.syncStore()

	store.readTrigger <- true
	return store, nil
}

// Close - closes all goroutines by sending signals over respective channels
// for goroutines to close themselves and at last closes teh Bolt database
func (s *Store) Close() error {
	s.stopStore <- true
	s.stopClean <- true
	s.stopRead <- true
	s.stopReport <- true
	s.stopSync <- true

	//drain events out so the readEvents goroutine can unblock and exit.
	//discard event read from bolt but still not delivered to the outside system
	//they will be sent on next application start- as thet are still sitting in bolt
	//because they are not confirmed to be delivered outside
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

// storeEvents runs in a goroutine and receives from s.eventsIn and saves events to bolt
// It waits for signal on s.stopStore channel to stop itself
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
					log.Errorf("go=store at=write-error error=%s", err)
				}
				e.saved <- err == nil
			}
		case <-s.stopStore:
			s.shutdown <- true
			return
		}

	}

}

// readEvents runs in a goroutine and reads events from Bolt and sends them on s.eventsOut
// it also sends event to itself via s.readTrigger to trigger new read attemps. This mechanism
// keeps it in busy loop until there are no events in boltdb
// It waits for signal on s.stopRead channel to stop itself
func (s *Store) readEvents() {
	for {
		select {
		case <-s.stopRead:
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
					log.Errorf("go=read at=read-error error=%s", err)
				}
			}
			// In case of Kafka death and birth again there was no way to read data stored in Bolt until next request was sent from outside
		case _, ok := <-time.After(5 * time.Second):

			if ok {
				if s.getWritePointer() > s.getReadPointer() {
					s.triggerRead()
				}
			}
		}

	}
}

// cleanEvents runs in a goroutine and  recieves from channels s.eventsDelivered and s.eventsFailed.
// for delivered events it removes the event from bolt
// for failed events it sets readPointer to the value of the failed event if the sequence of
// failed event is lower then readPointer so it can be read again
// It waits for signal on s.stopClean channel to stop itself
func (s *Store) cleanEvents() {
	for {
		select {
		case <-s.stopClean:
			s.shutdown <- true
			return
		case delivered, ok := <-s.eventsDelivered:
			if ok {
				// FIXME - DELETE if delivered%1000 == 0 {
				// 	log.Infof("go=clean at=delete sequence=%d", delivered)
				// }
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
}

// report  - logs read and write pointer numbers every 10 seconds
// This shows if there are any events in boltdb
// If boltdb is empty it will print equal numbers
// It waits for signal on s.stopReport channel to stop itself
func (s *Store) report() {
	for {
		select {
		case <-s.stopReport:
			// read := s.getReadPointer()
			// write := s.getWritePointer()
			// log.Infof("go=report at=shutdown-report read=%d write=%d delta=%d", read, write, write-read)
			s.shutdown <- true
			// log.Infof("go=report at=shutdown-report readFromStore=%d writtenToStore=%d delta=%d", s.readFromStore, s.writtenToStore, s.writtenToStore-s.readFromStore)
			return
		case _, ok := <-time.After(10 * time.Second):

			if ok {
				// read := s.getReadPointer()
				// write := s.getWritePointer()
				// log.Infof("go=report at=report read=%d write=%d delta=%d", read, write, write-read)
				log.Infof("go=report at=report readFromStore=%d writtenToStore=%d delta=%d", s.readFromStore, s.writtenToStore, s.writtenToStore-s.readFromStore)

			}
		}

	}

}

// writeEvent - store one event in Bolt
func (s *Store) writeEvent(seq int64, e *EventIn) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(defaultEventBucket)
		encoded, err := encodeEvent(e.event)
		if err != nil {
			log.Errorf("go=store at=encode-fail error=%s\n", err)
			return err
		}
		err = bucket.Put(writeSequence(seq), encoded)
		if err != nil {
			log.Errorf("go=store at=put-fail error=%s\n", err)
			return err
		}
		_ = s.incrementWrittenToStore()

		// if seq%1000 == 0 {
		// 	log.Infof("go=store at=wrote sequence=%d", seq)
		// }
		return nil

	})
	return err
}

// readEvent - read one event from bolt
func (s *Store) readEvent(seq int64) (*EventOut, error) {
	var eventOut *EventOut
	err := s.db.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket(defaultEventBucket)
		eventBytes := events.Get(writeSequence(seq))

		if eventBytes == nil || len(eventBytes) == 0 {
			return nil
		}

		_ = s.incrementReadFromStore()

		// if seq%1000 == 0 {
		// 	log.Infof("go=read at=read sequence=%d", seq)
		// }

		event, err := decodeEvent(eventBytes)
		if err != nil {
			log.Errorf("go=read at=decode-fail error=%s\n", err)
			return err
		}

		// FIXME - TEST BEGIN
		seqStr := strconv.FormatInt(seq, 10)
		eventAppend := append(event.Body, []byte("--"+seqStr)...)
		event.Body = eventAppend
		//FIXME TEST END

		eventOut = &EventOut{sequence: seq, event: event}
		return nil

	})
	return eventOut, err
}

// deleteEvent - delete one event from bolt
func (s *Store) deleteEvent(seq int64) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket(defaultEventBucket)
		err := events.Delete(writeSequence(seq))
		return err

	})
	if err != nil {
		log.Errorf("go=delete at=delete-error error=%s", err)
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
		s.readPointer++
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

	if s.writePointer == math.MaxInt64 {
		log.Errorf("go=incrementWritePointer Max number (MaxIni64): %d", s.writePointer)
		os.Exit(4)
	}

	s.writePointer++

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

// find: lowest sequence number in bolt (currentReadPointer) - that means earliest unread event to be sent outside
// and highest sequence number in bolt (lastWritePointer)- that means last written event number to bolt. Next write to bolt
// MUST be at lastWritePointer+1
func findReadAndWritePointers(db *bolt.DB) (int64, int64, error) {
	lastWritePointer := int64(0)
	currentReadPointer := int64(math.MaxInt64)
	err := db.View(func(tx *bolt.Tx) error {
		events := tx.Bucket(defaultEventBucket)
		err := events.ForEach(func(k, v []byte) error {
			seq, _ := readSequence(k)
			if seq > lastWritePointer {
				lastWritePointer = seq
			}
			if seq < currentReadPointer {
				currentReadPointer = seq
			}
			return nil
		})

		return err
	})

	if err != nil {
		return -1, -1, err
	}

	if currentReadPointer == int64(math.MaxInt64) {
		currentReadPointer = int64(1)
	}

	return lastWritePointer, currentReadPointer, nil
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
