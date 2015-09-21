event-shuttle
=============

WARNING: Work-in-progress now - as I learn golang on this example. It works now and can be tested.

goal: unix system service that collects events and reliably delivers them to kafka,
 relieving other services on the same system from having to do so.

journals events through bolt-db so that in the event of an kafka outage, events can still be accepted, and will be delivered when kafka becomes available.

* listens on 127.0.0.1:3887, rest-api is `POST /:topic -d message`
* journals events to bolt db.
* delivers events to kafka.


```
./event-shuttle --help
Reliably move events from source to destination. Use Bolt as a temporary local cache

Usage:
  event-shuttle [flags]
  event-shuttle [command]

Available Commands:
  version     Print the version number of event-shuttle

Flags:
      --allow-degraded-mode[=false]: allow to start without connection to Kafka - buffer everything locally waiting for Kafka to appear
  -c, --cpu=8: Number of CPU's to use
      --db="events.db": name of the boltdb database file
  -d, --debug[=false]: start a pprof http server on 6060 and set loglevel=debug
  -k, --kafka-brokers="192.168.99.100:9092": comma seperated list of ip:port to use as seed Kafka brokers
  -l, --log-level="warn": Log level - choose one of panic,fatal,error,warn|warning,info,debug
  -p, --port="3887": HTTP port on which to listen for events

Use "event-shuttle [command] --help" for more information about a command.
```

using a broker list
-------------------

`event-shuttle -brokers 1.2.3.4:9092,1.2.3.5:9092`
