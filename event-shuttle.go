package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	log "github.com/Sirupsen/logrus"

	"net/http"
)

const (
	AppName    = "event-shuttle"
	AppVersion = "0.1"
	AppDate    = "2015-09-17"

	DefaultKafkaBrokers      = "192.168.99.100:9092"
	DefaultBoltName          = "events.db"
	DefaultListeningHTTPPort = "3887"
	DefaultDebugMode         = false
	DefaultClientName        = AppName + AppVersion
	DefaultLogLevel          = "warn"
	DefaultAllowDegradedMode = false
)

var (
	exhibitor         bool
	kafkaBrokers      string
	db                string
	port              string
	debug             bool
	logLevel          string
	cpu               int
	allowDegradedMode bool

	degradedMode bool
	deliver      *KafkaDeliver
	store        *Store
	brokerList   []string
)

func runApp(cmd *cobra.Command, args []string) {

	// check logLevel values and set initLogLevel
	initLogLevel, err := log.ParseLevel(logLevel)
	if err != nil {
		fmt.Printf("Wrong loglevel value %s\n", logLevel)
		fmt.Println(cmd.Flags().Lookup("allow-degraded-mode").Usage)
		os.Exit(1)
	}
	initLog(initLogLevel)

	//  check debug
	if debug {
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
		initLogLevel = log.DebugLevel
	}

	//  make signal channels for gracefull termination
	exitChan := make(chan os.Signal)

	signal.Notify(exitChan, syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	// check db name & store init
	if db == "" {
		log.Printf("Db name can't be empty\n")
		log.Panicln(cmd.Flags().Lookup("db").Usage)
	}

	store, err := OpenStore(db)
	if err != nil {
		log.Printf("unable to open db: %s, exiting! %v\n", db, err)
		log.Panicln(cmd.Flags().Lookup("db").Usage)
	}

	// Kafka brokers & Kafka init

	if exhibitor {
		rb, err := KafkaSeedBrokers(os.Getenv("EXHIBITOR_URL"), "kafka")
		if err != nil {
			log.Printf("unable to get Kafka Seed Brokers, exiting! %v\n", err)
			log.Panicln(cmd.Flags().Lookup("exhibitor").Usage)
		}
		brokerList = rb
	} else {
		brokerList = strings.Split(kafkaBrokers, ",")
		if len(brokerList) == 0 {
			log.Printf("Broker list is empty or invalid format (%s)\n", kafkaBrokers)
			log.Panicln(cmd.Flags().Lookup("kafka-brokers").Usage)
		}
	}

	deliver, err = NewKafkaDeliver(store, DefaultClientName, brokerList)
	degradedMode = false

	if err != nil {
		if allowDegradedMode {
			log.Errorf("unable to create KafkaDeliver: %v, running in DEGRADED mode (saving events to Bolt)\n", err)
			degradedMode = true
		} else {
			log.Errorf("unable to create KafkaDeliver: %v. Degraded mode not allowed (see flags).\n", err)
			os.Exit(2)
		}
	}

	if !degradedMode {
		deliver.Start()
	}

	StartEndpoint(port, store)

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

func main() {

	AppCmd := &cobra.Command{
		Use:   AppName,
		Short: AppName + " moves events from HTTP source to Kafka destination",
		Long:  "Reliably move events from source to destination. Use Bolt as temporary local cache",
		Run: func(cmd *cobra.Command, args []string) {
			runApp(cmd, args)
		},
	}

	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version number of " + AppName,
		Long:  "All software has versions. This is " + AppName,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("%s version: %s (%s)\n", AppName, AppVersion, AppDate)
			os.Exit(0)
		},
	}

	AppCmd.Flags().BoolVarP(&exhibitor, "exhibitor", "e", false, "use EXHIBITOR_URL from env to lookup seed brokers")
	AppCmd.Flags().StringVarP(&kafkaBrokers, "kafka-brokers", "k", DefaultKafkaBrokers, "comma seperated list of ip:port to use as seed Kafka brokers")
	AppCmd.Flags().StringVarP(&db, "db", "", DefaultBoltName, "name of the boltdb database file")
	AppCmd.Flags().StringVarP(&port, "port", "p", DefaultListeningHTTPPort, "HTTP port on which to listen for events")
	AppCmd.Flags().BoolVarP(&debug, "debug", "d", DefaultDebugMode, "start a pprof http server on 6060 and set loglevel=debug")
	AppCmd.Flags().StringVarP(&logLevel, "log-level", "l", DefaultLogLevel, "Log level - choose one of panic,fatal,error,warn|warning,info,debug")
	AppCmd.Flags().IntVarP(&cpu, "cpu", "c", maxParallelism(), "Number of CPU's to use")
	AppCmd.Flags().BoolVarP(&allowDegradedMode, "allow-degraded-mode", "", DefaultAllowDegradedMode, "allow to start without connection to Kafka - buffer everything locally wating for Kafka to appear")

	AppCmd.AddCommand(versionCmd)
	AppCmd.Execute()

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

func maxParallelism() int {
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
