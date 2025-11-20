package main

import (
	"fmt"
	"net"
	// "encoding/json"
	// "os"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	fmt.Println("Escuchando en puerto 9000...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error aceptando conexión:", err)
			continue
		}

		fmt.Println("Cliente conectado desde:", conn.RemoteAddr())

		go handle(conn)
		// conn.Close() 
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	// Leer comando del buffer
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	// comando := string(buf[:n])
	input := string(buf[:n])
	line := strings.TrimSpace(input)
	partes := strings.SplitN(line, " ", 2)
	
	comando := partes[0]
	argumento := ""
	if len(partes) > 1 {
		argumento = partes [1]
	}

	switch comando {
		case "STORE":
			fmt.Println("Storing ", argumento)
		case "READ":
			fmt.Println("Reading ", argumento)

	}

}