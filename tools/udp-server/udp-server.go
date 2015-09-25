package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

/* A Simple function to verify error */
func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}

func report(i *uint64) {
	for {
		select {
		case _, ok := <-time.After(10 * time.Second):

			if ok {
				fmt.Printf("UDP Packets: %d\n", *i)
			}
		}
	}
}

func dosth(buf []byte, rlen int) {
	time.Sleep(time.Millisecond *5 )
}
func main() {
	/* Lets prepare a address at any address at port 10001*/
	ServerAddr, err := net.ResolveUDPAddr("udp", ":10001")
	CheckError(err)

	/* Now listen at selected port */
	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	ServerConn.SetReadBuffer(1024*1024*1024)
	ServerConn.SetWriteBuffer(1024*1024*1024)
	CheckError(err)
	defer ServerConn.Close()

	buf := make([]byte, 1024)
	var countUDP uint64 = 0

	go report(&countUDP)

	for {
		n, _, err := ServerConn.ReadFromUDP(buf)
		if n > 0 {
			countUDP++
		}
		go dosth(buf,n)
		//		fmt.Println("Received ", string(buf[0:n]))

		if err != nil {
			fmt.Println("Error: ", err)
		}
	}

}
