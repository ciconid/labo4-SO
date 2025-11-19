package main

import (
	"fmt"
	"net"
	"bufio"
	"os"
	"strings"
	// "encoding/json"
)

var name_node_socket = "192.168.100.174:9000"


func main() {
	reader := bufio.NewReader(os.Stdin)
	
	loop:
		for {
			fmt.Print("> ")
			
			input, _ := reader.ReadString('\n')
			command := strings.TrimSpace(input)
			
			switch command {
			case "exit":	
				break loop
			case "ls":
				ls()
				
			}
			
			// if command == "exit" {
			// 	break
			// }
			
			fmt.Println("Ejecutaste:", command)
		}


	// conn, err := net.Dial("tcp", name_node_socket)
	// if err != nil {
	// 	panic(err)
	// }
	// defer conn.Close()

	// fmt.Println("Conectado al servidor!")
}

func ls() {
	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Conectado al servidor!")

	// Enviar comando
	conn.Write([]byte("LISTAR"))

	// Leer respuesta
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)

	var lista []string
	json.Unmarshal(buf[:n], &lista)

	fmt.Println("Archivos recibidos:", lista)
}