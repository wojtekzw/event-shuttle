package main

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

const maxTestCount = 1000000

func main() {
	ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10001")
	CheckError(err)

	LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	CheckError(err)

	Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	Conn.SetReadBuffer(1024*1024*1024)
	Conn.SetWriteBuffer(1024*1024*1024)

	CheckError(err)

	defer Conn.Close()

	start := time.Now()
	for i := 0; i < maxTestCount; i++ {
		msg := strconv.Itoa(i)
		//i++
		buf := []byte(msg)
		_, err := Conn.Write(buf)
		if err != nil {
			fmt.Println(msg, err)
		}
		time.Sleep(time.Nanosecond * 100)
	}

	timeDiff := time.Since(start)
	timeDiffInSec := float64(timeDiff) * 1e-9
	fmt.Printf("Excecution time:%f sec, %f ops/s\n", timeDiffInSec, maxTestCount/timeDiffInSec)
}
