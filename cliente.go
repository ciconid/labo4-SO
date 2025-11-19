package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "192.168.100.96:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Conectado al servidor!")
}
