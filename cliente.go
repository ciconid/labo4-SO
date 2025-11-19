package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "10.0.2.15:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Conectado al servidor!")
}
