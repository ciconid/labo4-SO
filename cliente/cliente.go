package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

type BlockInfo struct {
	Block string `json:"block"`
	Node  string `json:"node"`
}

var name_node_socket = "192.168.100.174:9000"

func main() {
	reader := bufio.NewReader(os.Stdin)

loop:
	for {
		fmt.Print("> ")

		input, _ := reader.ReadString('\n')
		// command := strings.TrimSpace(input)
		line := strings.TrimSpace(input)
		partes := strings.SplitN(line, " ", 2)

		command := partes[0]
		argumento := ""
		if len(partes) > 1 {
			argumento = partes[1]
		}

		switch command {
		case "exit":
			break loop
		case "ls":
			ls()
		case "put":
			put(argumento)
		case "get":
			get()
		case "info":
			// fmt.Printf("Command: %s - Arg: %s \n", command, argumento)
			info(argumento)
		default:
			fmt.Println("Comando inválido")
		}

		// if command == "exit" {
		// 	break
		// }

		// fmt.Println("Ejecutaste:", command)
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

	// fmt.Println("Conectado al servidor!")

	// Enviar comando
	conn.Write([]byte("LISTAR"))

	// Leer respuesta
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)

	var lista []string
	json.Unmarshal(buf[:n], &lista)

	// fmt.Println("Archivos recibidos:", lista)
	for _, item := range lista {
		fmt.Println(item)
	}
}

func put(argumento string) {
	fmt.Println("Hola desde put")
	// simulo que se parte el archivo original en varios bloques------------------------------------MODIF
	cantBloques := 7

	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Enviar comando
	comando := fmt.Sprintf("PUT %s %d", argumento, cantBloques)
	conn.Write([]byte(comando))

	// conn, err := net.Dial("tcp", name_node_socket)
	// if err != nil {
	// 	panic(err)
	// }
	// defer conn.Close()

	// // Enviar comando
	// comando := fmt.Sprintf("STORE %s", argumento)
	// conn.Write([]byte(comando))

	// Leer respuesta
	// buf := make([]byte, 2048)
	// n, _ := conn.Read(buf)
}

func get() {
	fmt.Println("Hola desde get")
}

func info(argumento string) {
	// fmt.Println("Hola desde info")
	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Enviar comando
	comando := fmt.Sprintf("INFO %s", argumento)
	conn.Write([]byte(comando))

	// Leer respuesta
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)

	var lista []BlockInfo
	json.Unmarshal(buf[:n], &lista)
	// if err != nil {
	// 	fmt.Println("Error al parsear:", err)
	// 	return
	// }

	// fmt.Println("lista: ", lista)
	// fmt.Println("len(lista): ", len(lista))

	for _, item := range lista {
		fmt.Println("Block:", item.Block, "- Node:", item.Node)
		// fmt.Println(item)
	}
}
