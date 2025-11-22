package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

type BlockInfo struct {
	Block string `json:"block"`
	Node  string `json:"node"`
}

type BloqueAsignado struct {
	Block      int    `json:"block"`
	DataNodeIP string `json:"data_node_ip"`
}

// var name_node_socket = "192.168.100.174:9000" //windows
var name_node_socket = "192.168.100.77:9000" //notebook

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
	// cantBloques := 7
	// var bloques = []string{
	// 	"Bloque 1",
	// 	"Bloque 2",
	// 	"Bloque 3",
	// 	"Bloque 4",
	// 	"Bloque 5",
	// 	"Bloque 6",
	// 	"Bloque 7",
	// }

	// Dividir archivo original en bloques
	partesArgumento := strings.SplitN(argumento, " ", 2)
	nombreArchivo := partesArgumento[0]
	tamanioBloque := 1024
	// nombreArchivo := "lotr.txt"
	bloques, err := LeeArchivoEnBloques(nombreArchivo, tamanioBloque)
	if err != nil {
		panic(err)
	}
	cantBloques := len(bloques)

	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Enviar comando
	comando := fmt.Sprintf("PUT %s %d", argumento, cantBloques)
	conn.Write([]byte(comando))

	// Leer respuesta
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)

	var bloquesAsignados []BloqueAsignado
	json.Unmarshal(buf[:n], &bloquesAsignados)

	// Conexiones con cada DataNode para enviar bloque
	for _, item := range bloquesAsignados {
		fmt.Println()
		// fmt.Println("Enviar bloque", item.Block, "contenido", string(bloques[item.Block-1]), "a DataNodeIP:", item.DataNodeIP)
		conn, err := net.Dial("tcp", item.DataNodeIP)

		if err != nil {
			panic(err)
		}

		// Enviar comando
		comando := fmt.Sprintf("STORE b%d_%s %s", item.Block, nombreArchivo, bloques[item.Block-1])
		// fmt.Println(comando)
		conn.Write([]byte(comando))

		conn.Close()
	}

	// avisar a NameNode que PUT fue exitoso (para poder guardar la info en metadata.json)
	msg := "TRANSFER_COMPLETE\n"
	_, err = conn.Write([]byte(msg))
	if err != nil {
		panic(err)
	}

}

// LeeArchivoEnBloques lee un archivo y lo divide en bloques de tamaño blockSize.
// Devuelve un slice con cada bloque como []byte.
func LeeArchivoEnBloques(filePath string, blockSize int) ([][]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buffer := make([]byte, blockSize)
	var blocks [][]byte

	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Copiamos el bloque real leído
		block := make([]byte, n)
		copy(block, buffer[:n])

		blocks = append(blocks, block)
	}

	return blocks, nil
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
