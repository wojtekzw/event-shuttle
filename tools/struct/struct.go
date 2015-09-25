package main

import (
	"encoding/json"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
)

type Room struct {
	Size int `json:"rozmiar"`
}

func (r Room) ShowSize() {
	fmt.Println("Room size is:", r.Size)
}

type Kitchen struct {
	Room
	Plates int `json:"taleze"`
	Forks  int `json:"widelce"`
	Knives int `json:"noze"`
}

func (k Kitchen) ShowSize() {
	fmt.Println("Kitchen size is:", k.Size)
}

func main() {

	k := Kitchen{Room{14}, 1, 2, 3}
	k.ShowSize()
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.JSONFormatter{})
	j, _ := json.Marshal(k)
	log.Errorf("%v", k)
	log.Errorf("%s", string(j))
}
