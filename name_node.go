package main

import (
	"fmt"
	"net"
	"encoding/json"
)

var lista_de_archivos = []string{"archivo1.txt", "archivo2.txt"} // esta lista la tiene que generar el propio name_node

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

	// Leer comando simple
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	comando := string(buf[:n])

	if comando == "LISTAR" {
		// Serializar a JSON
		jsonData, _ := json.Marshal(lista_de_archivos)
		conn.Write(jsonData)
	}
}