package main

import (
	"fmt"
	"net"
	"bufio"
	"os"
	"strings"
)

var nameNodeSocket = "192.168.100.96:9000"

func main() {
	reader := bufio.NewReader(os.Stdin)
	
	for {
		fmt.Print("> ")
		
		input, _ := reader.ReadString('\n')
		command := strings.TrimSpace(input)
		
		if command == "exit" {
			break
		}
		
		fmt.Println("Ejecutaste:", command)
	}


	conn, err := net.Dial("tcp", nameNodeSocket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Conectado al servidor!")
}

func ls() {
	conn, err := net.Dial("tcp", nameNodeSocket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Conectado al servidor!")
}