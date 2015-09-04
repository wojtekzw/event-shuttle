package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"

	"net/http"
)

func main() {

	runtime.GOMAXPROCS(2)
	log.Debugf("at=main, MaxParallelism=%d\n", MaxParallelism())

	exhibitor := flag.Bool("exhibitor", false, "use EXHIBITOR_URL from env to lookup seed brokers")
	brokers := flag.String("brokers", "", "comma seperated list of ip:port to use as seed brokers")
	db := flag.String("db", "events.db", "name of the boltdb database file")
	port := flag.String("port", "3887", "port on which to listen for events")
	debug := flag.Bool("debug", false, "start a pprof http server on 6060")

	flag.Parse()
	// logLevel := log.ErrorLevel
	logLevel := log.DebugLevel

	if *debug {
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
		logLevel = log.DebugLevel
	}

	initLog(logLevel)

	exitChan := make(chan os.Signal)

	signal.Notify(exitChan, syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	store, err := OpenStore(*db)
	if err != nil {
		log.Panicf("unable to open %s, exiting! %v\n", *db, err)
	}

	var brokerList []string

	if *exhibitor {
		rb, err := KafkaSeedBrokers(os.Getenv("EXHIBITOR_URL"), "kafka")
		if err != nil {
			log.Panicf("unable to get Kafka Seed Brokers, exiting! %v\n", err)
		}
		brokerList = rb
	} else {
		brokerList = strings.Split(*brokers, ",")
	}

	deliver, err := NewKafkaDeliver(store, "test", brokerList)
	degradedMode := false
	if err != nil {
		log.Errorf("unable to create KafkaDeliver: %v, runing in DEGRADED mode (saving events to BoldDB)\n", err)
		degradedMode = true
	}

	if !degradedMode {
		deliver.Start()
	}
	StartEndpoint(*port, store)

	select {
	case sig := <-exitChan:
		log.Debugf("go=main at=received-signal signal=%s\n", sig)
		err := store.Close()
		if !degradedMode {
			deliver.Stop()
		}
		if err != nil {
			log.Fatalf("go=main at=store-close-error error=%s\n", err)
		} else {
			log.Debugf("go=main at=store-closed-cleanly \n")
		}
	}

}

type EventIn struct {
	event *Event
	saved chan bool
}

type EventOut struct {
	event    *Event
	sequence int64
}

type Event struct {
	Channel string
	Body    []byte
}

func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

func initLog(logLevel log.Level) {
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})

	// Use the Airbrake hook to report errors that have Error severity or above to
	// an exception tracker. You can create custom hooks, see the Hooks section.
	// log.AddHook(airbrake.NewHook("https://example.com", "xyz", "development"))

	// Output to stderr instead of stdout, could also be a file.
	log.SetOutput(os.Stderr)

	// Only log the warning severity or above.
	log.SetLevel(logLevel)
}
