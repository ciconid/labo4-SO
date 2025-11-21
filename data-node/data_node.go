package main

import (
	"fmt"
	"net"
	// "encoding/json"
	"os"
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
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	// comando := string(buf[:n])
	input := string(buf[:n])
	fmt.Println()
	fmt.Println(input)
	fmt.Println()
	line := strings.TrimSpace(input)
	partes := strings.SplitN(line, " ", 2)
	
	comando := partes[0]
	argumento := ""
	if len(partes) > 1 {
		argumento = partes [1]
	}

	switch comando {
		case "STORE":
			fmt.Println()
			fmt.Println()
			fmt.Println("Storing ")
			
			partesArgumento := strings.SplitN(argumento, " ", 2)
			nombreArchivo := partesArgumento[0]
			contenidoArchivo := partesArgumento[1]

			fmt.Println(nombreArchivo)
			fmt.Println(contenidoArchivo)

			writePath := fmt.Sprintf("./blocks/%s", nombreArchivo)

			err := os.WriteFile(writePath, []byte(contenidoArchivo), 0644)
			if err != nil {
				fmt.Println("Error al escribir archivos", err)
			}

			
			/* fmt.Println(argumento) */

		case "READ":
			fmt.Println("Reading ", argumento)

	}

}