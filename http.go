package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"

	//"github.com/go-zoo/bone"
	"github.com/bmizerany/pat"
)

// HTTPEndpoint -  struct with methods to start HTTP server
type HTTPEndpoint struct {
	store *Store
}

// StartEndpoint - starts HTTP server on localhost
func StartEndpoint(port string, store *Store) *HTTPEndpoint {
	endpoint := HTTPEndpoint{store: store}
	mux := pat.New()
	// mux := bone.New()
	mux.Post(fmt.Sprintf("/:topic"), http.HandlerFunc(endpoint.postEvent))
	go http.ListenAndServe("127.0.0.1:"+port, mux)
	return &endpoint
}

func (e *HTTPEndpoint) postEvent(w http.ResponseWriter, req *http.Request) {
	channel := req.URL.Query().Get(":topic")
	if channel == "" {
		w.WriteHeader(400)
		w.Write(noChannel)
	} else {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(500)
			w.Write(bodyErr)
		} else {
			saved := make(chan bool)
			event := EventIn{event: &Event{Channel: channel, Body: body}, saved: saved}
			e.sendEvent(&event)
			timeout := time.After(1000 * time.Millisecond)
			select {
			case ok := <-saved:
				if ok {
					w.WriteHeader(200)
				} else {
					w.WriteHeader(500)
					w.Write(saveErr)
				}
			case <-timeout:
				log.Infoln("at=post-event-timeout")
				w.WriteHeader(500)
				w.Write(saveTimeout)
			}

		}
	}
}

func (e *HTTPEndpoint) sendEvent(event *EventIn) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorln("at=recover-send-event-panic")
			//if we get here, we are shutting down, but the recover stops it, so exit
			os.Exit(2)
		}
	}()
	// the store owns the in channel and can close it on shutdown
	// so we wrap this call which can panic in a recover
	e.send(event)
}

func (e *HTTPEndpoint) send(event *EventIn) {
	e.store.eventsInChannel() <- event
}

type errJSON struct {
	id      string
	message string
}

func convertToJSON(err errJSON) []byte {
	j, _ := json.Marshal(err)
	return j
}

var noChannel = convertToJSON(errJSON{id: "no-channel", message: "no event channel specified"})
var bodyErr = convertToJSON(errJSON{id: "read-error", message: "error while reading body"})
var saveErr = convertToJSON(errJSON{id: "save-error", message: "save event returned false"})
var saveTimeout = convertToJSON(errJSON{id: "save-timeout", message: "save timed out"})
