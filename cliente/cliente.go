package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
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
			get(argumento)
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

func get(nombreArchivo string) {
	fmt.Println("Hola desde get")
	var lista []BlockInfo = recuperarInfoDeArchivo(nombreArchivo)

	for _, item := range lista {
		fmt.Println("Block:", item.Block, "- Node:", item.Node)
	}

	// Necesitamos saber cuántos bloques habrá para crear el slice final
	bloquesRecuperados := make([][]byte, len(lista))

	for _, info := range lista {
		// Convertir el Block (string) → índice int
		numBloque, err := strconv.Atoi(info.Block)
		if err != nil {
			fmt.Println("Block inválido:", info.Block)
			continue
		}
		indice := numBloque - 1 // bloque 1 → índice 0

		// 1. Conectar al DataNode
		conn, err := net.Dial("tcp", info.Node)
		if err != nil {
			fmt.Println("Error conectando a", info.Node, ":", err)
			continue
		}

		func() {
			defer conn.Close()

			// 2. Solicitar el bloque
			req := fmt.Sprintf("READ b%s_%s\n", info.Block, nombreArchivo)
			_, err = conn.Write([]byte(req))
			if err != nil {
				fmt.Println("Error enviando solicitud:", err)
				return
			}

			// 3. Leer el bloque completo
			data, err := io.ReadAll(conn)
			if err != nil {
				fmt.Println("Error leyendo bloque", info.Block, ":", err)
				return
			}

			fmt.Println()
			fmt.Printf("Bloque %s recibido (%d bytes)\n", info.Block, len(data))

			// 4. Guardar en la posición correcta del slice
			bloquesRecuperados[indice] = data

			// fmt.Println("\n", bloquesRecuperados)
		}()
	}

	err := reconstruirArchivo(nombreArchivo, bloquesRecuperados)
	if err != nil {
		fmt.Println("Error reconstruyendo archivo:", err)
	} else {
		fmt.Println("Archivo reconstruido con éxito")
	}

}

func reconstruirArchivo(nombreArchivo string, bloques [][]byte) error {
	// Crear buffer donde se unirá todo
	var final []byte

	// Concatenar bloques en orden
	for i, bloque := range bloques {
		if bloque == nil {
			return fmt.Errorf("el bloque %d está vacío o no fue recuperado", i+1)
		}
		final = append(final, bloque...)
	}

	// Guardar en el archivo final
	outputPath := fmt.Sprintf("./recuperados/%s", nombreArchivo)

	err := os.WriteFile(outputPath, final, 0644)
	if err != nil {
		return err
	}

	fmt.Println("Archivo reconstruido correctamente en:", outputPath)
	return nil
}

func info(argumento string) {
	lista := recuperarInfoDeArchivo(argumento)

	for _, item := range lista {
		fmt.Println("Block:", item.Block, "- Node:", item.Node)
	}
}

func recuperarInfoDeArchivo(nombreArchivo string) []BlockInfo {
	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Enviar comando
	comando := fmt.Sprintf("INFO %s", nombreArchivo)
	conn.Write([]byte(comando))

	// Leer respuesta
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)

	var lista []BlockInfo
	json.Unmarshal(buf[:n], &lista)

	return lista
}
