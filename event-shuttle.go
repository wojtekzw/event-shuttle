package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	log "github.com/Sirupsen/logrus"

	"net/http"
)

// Main defaults for application
const (
	AppName    = "event-shuttle"
	AppVersion = "0.2"
	AppDate    = "2015-09-17"

	DefaultKafkaBrokers      = "192.168.99.100:9092"
	DefaultBoltName          = "events.db"
	DefaultListeningHTTPPort = "3887"
	DefaultDebugMode         = false
	DefaultClientName        = AppName + AppVersion
	DefaultLogLevel          = "warn"
	DefaultAllowDegradedMode = false
)

type appConfig struct {
	kafkaBrokers    string
	kafkaBrokerList []string

	db                string
	port              string
	debug             bool
	logLevel          string
	initLogLevel      log.Level
	cpu               int
	allowDegradedMode bool
}

type appRuntime struct {
	appConfig
	degradedMode      bool
	exitChan          chan os.Signal
	stopReconnect     chan bool
	shutdownReconnect chan bool
	deliver           *KafkaDeliver
	store             *Store
}

func (a *appConfig) validateConfig(cmd *cobra.Command) error {
	var err error

	// check logLevel values and set initLogLevel
	a.initLogLevel, err = log.ParseLevel(a.logLevel)
	if err != nil {
		err = fmt.Errorf("Invalid log level: \"%s\"\n%s", a.logLevel, cmd.Flags().Lookup("log-level").Usage)
		return err
	}

	// check db name
	if a.db == "" {
		err = fmt.Errorf("Db name can't be empty\n%s", cmd.Flags().Lookup("db").Usage)
		return err
	}

	// Kafka brokers
	a.kafkaBrokerList = strings.Split(a.kafkaBrokers, ",")
	if len(a.kafkaBrokerList) == 0 {
		err = fmt.Errorf("Broker list is empty or invalid format (%s)\n%s", a.kafkaBrokers, cmd.Flags().Lookup("kafka-brokers").Usage)
		return err
	}

	return nil
}

func (a *appRuntime) initApp() error {
	a.stopReconnect = make(chan bool)
	a.shutdownReconnect = make(chan bool)
	return nil
}

func (a *appRuntime) endApp() error {
	return nil
}

func (a *appRuntime) debugApp() error {

	//  check debug
	if a.debug {
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
		a.initLogLevel = log.DebugLevel
	}

	return nil
}

func (a *appRuntime) catchOSSignals() {
	//  make signal channel for gracefull termination
	a.exitChan = make(chan os.Signal)

	signal.Notify(a.exitChan, syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

}

func (a *appRuntime) setLogLevel() {
	initLog(a.initLogLevel)

}

func (a *appRuntime) runApp(cmd *cobra.Command, args []string) {

	var err error

	a.catchOSSignals()

	a.setLogLevel()

	a.store, err = OpenStore(a.db)
	if err != nil {
		log.Printf("unable to open db: %s, exiting! %v\n", a.db, err)
		log.Panicln(cmd.Flags().Lookup("db").Usage)
	}

	// Kafka brokers & Kafka init

	a.deliver, err = NewKafkaDeliver(a.store, DefaultClientName, a.kafkaBrokerList)
	a.degradedMode = false

	if err != nil {
		if a.allowDegradedMode {
			log.Errorf("unable to create KafkaDeliver: %v, running in DEGRADED mode (saving events to Bolt)\n", err)
			a.degradedMode = true

		} else {
			log.Errorf("unable to create KafkaDeliver: %v. Degraded mode not allowed (see flags).\n", err)
			os.Exit(2)
		}
	}

	go a.reconnectDelivery()

	if !a.degradedMode {
		a.deliver.Start()
	}

	StartEndpoint(a.port, a.store)

	select {

	case sig := <-a.exitChan:

		// Signal reconnectDeliver to stop itself
		a.stopReconnect <- true
		// wait for response
		<-a.shutdownReconnect
		log.Debugf("go=main at=received-signal signal=%s\n", sig)

		err := a.store.Close()

		if !a.degradedMode {
			a.deliver.Stop()
		}
		if err != nil {
			log.Fatalf("go=main at=store-close-error error=%s\n", err)
		} else {
			log.Debugf("go=main at=store-closed-cleanly \n")
		}
	}

}

// FIXME - try to reconnect ONLY if started in DEGRADED mode
// Does nothing if Kafka goes away during operation
// TODO - Uses channel stopReconnect which is in delivery and should be in App !!!!!!!!
func (a *appRuntime) reconnectDelivery() {
	var err error

	for {
		select {
		case <-a.stopReconnect:
			log.Debugf("go=main at=reconnect-pre-shutdown \n")
			a.shutdownReconnect <- true
			log.Debugf("go=main at=reconnect-shutdown \n")
			return
		case _, ok := <-time.After(10 * time.Second):
			log.Debugf("go=main at=reconnect-time-10s \n")
			if ok && a.degradedMode {

				a.deliver, err = NewKafkaDeliver(a.store, DefaultClientName, a.kafkaBrokerList)
				if err == nil {
					a.deliver.Start()
					a.degradedMode = false
				}

			}
		}
	}

}

func main() {

	var (
		err    error
		appRun appRuntime
	)

	appRun = appRuntime{}

	AppCmd := &cobra.Command{
		Use:   AppName,
		Short: AppName + " moves events from HTTP source to Kafka destination",
		Long:  "Reliably move events from source to destination. Use Bolt as a temporary local cache",
		Run: func(cmd *cobra.Command, args []string) {
			appRun.initApp()
			err = appRun.validateConfig(cmd)
			if err != nil {
				fmt.Printf("%s\n", err)
				os.Exit(1)
			}
			appRun.debugApp()
			appRun.runApp(cmd, args)
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

	AppCmd.Flags().StringVarP(&appRun.kafkaBrokers, "kafka-brokers", "k", DefaultKafkaBrokers, "comma seperated list of ip:port to use as seed Kafka brokers")
	AppCmd.Flags().StringVarP(&appRun.db, "db", "", DefaultBoltName, "name of the boltdb database file")
	AppCmd.Flags().StringVarP(&appRun.port, "port", "p", DefaultListeningHTTPPort, "HTTP port on which to listen for events")
	AppCmd.Flags().BoolVarP(&appRun.debug, "debug", "d", DefaultDebugMode, "start a pprof http server on 6060 and set loglevel=debug")
	AppCmd.Flags().StringVarP(&appRun.logLevel, "log-level", "l", DefaultLogLevel, "Log level - choose one of panic,fatal,error,warn|warning,info,debug")
	AppCmd.Flags().IntVarP(&appRun.cpu, "cpu", "c", maxParallelism(), "Number of CPU's to use")
	AppCmd.Flags().BoolVarP(&appRun.allowDegradedMode, "allow-degraded-mode", "", DefaultAllowDegradedMode, "allow to start without connection to Kafka - buffer everything locally waiting for Kafka to appear")

	AppCmd.AddCommand(versionCmd)

	AppCmd.Execute()

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

	// Output to stderr instead of stdout, could also be a file.
	log.SetOutput(os.Stderr)

	// Only log the warning severity or above.
	log.SetLevel(logLevel)
}
