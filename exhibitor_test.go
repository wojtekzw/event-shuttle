package main

import (
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"

	"github.com/bmizerany/assert"
)

func init() {
	initLog(log.DebugLevel)
}

func TestExhibitor(t *testing.T) {
	url := os.Getenv("EXHIBITOR_URL")
	if url == "" {
		log.Errorln("EXHIBITOR_URL not set, skipping TestExhibitor")
		return
	} else {
		brokers, err := KafkaSeedBrokers(url, "kafka")
		assert.T(t, err == nil, err)
		log.Debugf("brokers: %s", brokers)
	}
}
