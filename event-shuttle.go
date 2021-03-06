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
	"github.com/spf13/viper"

	log "github.com/Sirupsen/logrus"

	"net/http"
)

// Main defaults for application
const (
	AppName    = "event-shuttle"
	AppNameEnv = "SHUTTLE"
	AppVersion = "0.3"
	AppDate    = "2015-09-17"

	DefaultKafkaBrokers      = "localhost:9092"
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

func (a *appConfig) getConfigStr() string {
	return fmt.Sprintf("%#v", a)
}

func (a *appConfig) getAndValidateConfig(cmd *cobra.Command) error {
	var err error

	a.port = viper.GetString("port")
	a.debug = viper.GetBool("debug")
	a.cpu = viper.GetInt("cpu")
	a.allowDegradedMode = viper.GetBool("allow-degraded-mode")

	// check logLevel values and set initLogLevel
	a.logLevel = viper.GetString("log-level")
	a.initLogLevel, err = log.ParseLevel(a.logLevel)
	if err != nil {
		err = fmt.Errorf("Invalid log level: \"%s\"\n%s", a.logLevel, cmd.Flags().Lookup("log-level").Usage)
		return err
	}

	// check db name
	a.db = viper.GetString("db")
	if a.db == "" {
		err = fmt.Errorf("Db name can't be empty\n%s", cmd.Flags().Lookup("db").Usage)
		return err
	}

	// Kafka brokers
	a.kafkaBrokerList = strings.Split(viper.GetString("kafka-brokers"), ",")
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
		log.Errorf("unable to open db: %s, exiting! %v\n", a.db, err)
		os.Exit(2)
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
			os.Exit(3)
		}
	}

	go a.reconnectDelivery()

	if !a.degradedMode {
		a.deliver.Start()
	}

	StartEndpoint(a.port, a.store)

	// wait for end signal
	select {

	case sig := <-a.exitChan:
		log.Debugf("Got signal %v. Exiting...", sig)
		// Signal reconnectDeliver to stop itself
		a.stopReconnect <- true
		// wait for response
		<-a.shutdownReconnect

		err := a.store.Close()

		if !a.degradedMode {
			a.deliver.Stop()
		}
		if err != nil {
			log.Errorf("go=main at=store-close-error error=%s\n", err)
			os.Exit(1)
		}
	}

}

// reconnectDelivery - try to reconnect ONLY if started in DEGRADED mode
// Does nothing if Kafka goes away during operation
func (a *appRuntime) reconnectDelivery() {
	var err error

	for {
		select {
		case <-a.stopReconnect:
			a.shutdownReconnect <- true
			return
		case _, ok := <-time.After(10 * time.Second):
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

	viper.SetEnvPrefix(AppNameEnv)
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	appRun = appRuntime{}

	AppCmd := &cobra.Command{
		Use:   AppName,
		Short: AppName + " moves events from HTTP source to Kafka destination",
		Long:  "Reliably move events from source to destination. Use Bolt as a temporary local cache",
		Run: func(cmd *cobra.Command, args []string) {
			appRun.initApp()
			err = appRun.getAndValidateConfig(cmd)
			fmt.Println(appRun.getConfigStr())
			// os.Exit(1)
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

	AppCmd.Flags().StringVarP(&appRun.kafkaBrokers, "kafka-brokers", "k", DefaultKafkaBrokers, "comma seperated list of ip:port to use as seed Kafka brokers"+" (env: "+AppNameEnv+"_KAFKA_BROKERS"+")")
	AppCmd.Flags().StringVarP(&appRun.db, "db", "", DefaultBoltName, "name of the boltdb database file"+" (env: "+AppNameEnv+"_DB"+")")
	AppCmd.Flags().StringVarP(&appRun.port, "port", "p", DefaultListeningHTTPPort, "HTTP port on which to listen for events"+" (env: "+AppNameEnv+"_PORT"+")")
	AppCmd.Flags().BoolVarP(&appRun.debug, "debug", "d", DefaultDebugMode, "start a pprof http server on 6060 and set loglevel=debug"+" (env: "+AppNameEnv+"_DEBUG"+")")
	AppCmd.Flags().StringVarP(&appRun.logLevel, "log-level", "l", DefaultLogLevel, "Log level - choose one of panic,fatal,error,warn|warning,info,debug"+" (env: "+AppNameEnv+"_LOG_LEVEL"+")")
	AppCmd.Flags().IntVarP(&appRun.cpu, "cpu", "c", maxParallelism(), "Number of CPU's to use"+" (env: "+AppNameEnv+"_CPU"+")")
	AppCmd.Flags().BoolVarP(&appRun.allowDegradedMode, "allow-degraded-mode", "", DefaultAllowDegradedMode, "allow to start without connection to Kafka - buffer everything locally waiting for Kafka to appear"+" (env: "+AppNameEnv+"_ALLOW_DEGRADED_MODE"+")")

	viper.BindPFlag("kafka-brokers", AppCmd.Flags().Lookup("kafka-brokers"))
	viper.BindPFlag("db", AppCmd.Flags().Lookup("db"))
	viper.BindPFlag("port", AppCmd.Flags().Lookup("port"))
	viper.BindPFlag("debug", AppCmd.Flags().Lookup("debug"))
	viper.BindPFlag("log-level", AppCmd.Flags().Lookup("log-level"))
	viper.BindPFlag("cpu", AppCmd.Flags().Lookup("cpu"))
	viper.BindPFlag("allow-degraded-mode", AppCmd.Flags().Lookup("allow-degraded-mode"))

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
