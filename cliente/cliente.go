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
	"time"
)

type BlockInfo struct {
	Block string `json:"block"`
	Node  string `json:"node"`
}

var name_node_socket = "192.168.100.77:9000"

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Comandos disponibles:")
	fmt.Println("  ls                → listar archivos")
	fmt.Println("  put <archivo>     → subir archivo")
	fmt.Println("  get <archivo>     → descargar archivo")
	fmt.Println("  info <archivo>    → ver metadata")
	fmt.Println("  help              → mostrar esta ayuda")
	fmt.Println()

loop:
	for {
		fmt.Print("> ")

		input, _ := reader.ReadString('\n')
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
			if argumento != "" {
				fmt.Println("Uso incorrecto del comando")
				fmt.Println("Uso: > ls")
				break
			}
			ls()
		case "put":
			if argumento == "" {
				fmt.Println("Uso incorrecto del comando")
				fmt.Println("Uso: > put <archivo>")
				break
			}
			put(argumento)
		case "get":
			if argumento == "" {
				fmt.Println("Uso incorrecto del comando")
				fmt.Println("Uso: > get <archivo>")
				break
			}
			get(argumento)
		case "info":
			if argumento == "" {
				fmt.Println("Uso incorrecto del comando")
				fmt.Println("Uso: > info <archivo>")
				break
			}
			info(argumento)
		case "help":
			if argumento != "" {
				fmt.Println("Uso incorrecto del comando")
				fmt.Println("Uso: > help")
				break
			}
			fmt.Println("Comandos disponibles:")
			fmt.Println("  ls                → listar archivos")
			fmt.Println("  put <archivo>     → subir archivo")
			fmt.Println("  get <archivo>     → descargar archivo")
			fmt.Println("  info <archivo>    → ver metadata")
			fmt.Println("  help              → mostrar esta ayuda")
		default:
			fmt.Println("Comando inválido")
		}
	}
}

func ls() {
	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		fmt.Println("Error al conectar con name_node", name_node_socket)
		return
	}
	defer conn.Close()

	// Enviar comando
	conn.Write([]byte("LISTAR"))

	// Leer respuesta
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)

	// Procesar y mostrar respuesta
	var lista []string
	json.Unmarshal(buf[:n], &lista)

	for _, item := range lista {
		fmt.Println(item)
	}
}

func put(argumento string) {
	abort := false

	// Dividir archivo original en bloques
	partesArgumento := strings.SplitN(argumento, " ", 2)
	nombreArchivo := partesArgumento[0]
	tamanioBloque := 1024

	bloques, err := LeeArchivoEnBloques(nombreArchivo, tamanioBloque)
	if err != nil {
		fmt.Println("CLIENTE-PUT: Error al leer archivo", nombreArchivo)
		fmt.Println("Abortando PUT de", nombreArchivo)
		return
	}
	cantBloques := len(bloques)

	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		fmt.Println("Error al conectar con name_node", name_node_socket)
		return
	}
	defer conn.Close()

	// Enviar comando
	comando := fmt.Sprintf("PUT %s %d", argumento, cantBloques)
	conn.Write([]byte(comando))

	// Leer respuesta
	buf := make([]byte, 2048)
	n, _ := conn.Read(buf)

	var bloquesAsignados []BlockInfo
	json.Unmarshal(buf[:n], &bloquesAsignados)

	// Conexiones con cada DataNode para enviar bloque
	for _, item := range bloquesAsignados {
		func() {
			timeout := 2 * time.Second
			conn, err := net.DialTimeout("tcp", item.Node, timeout)
			if err != nil {
				fmt.Println("CLIENTE-PUT: Error conexion con el nodo", item.Node)
				fmt.Println("Abortando PUT de", nombreArchivo)
				abort = true
				return
			}
			defer conn.Close()

			// Enviar comando
			blockIndex, err := strconv.Atoi(item.Block)
			if err != nil {
				fmt.Println("Error al convertir de string a int")
				abort = true
				return
			}
			comando := fmt.Sprintf("STORE b%s_%s %s", item.Block, nombreArchivo, bloques[blockIndex-1])
			conn.Write([]byte(comando))
		}()

		if abort {
			break
		}
	}

	if abort {
		return
	}

	// avisar a NameNode que PUT fue exitoso (para poder guardar la info en metadata.json)
	msg := "TRANSFER_COMPLETE\n"
	_, err = conn.Write([]byte(msg))
	if err != nil {
		fmt.Println("CLIENTE-PUT: Error al enviar TRANSFER_COMPLETE al name_node")
		return
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
	fmt.Println()

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
		timeout := 2 * time.Second
		conn, err := net.DialTimeout("tcp", info.Node, timeout)
		if err != nil {
			fmt.Println("Error conectando a", info.Node, ":", err)
			fmt.Println("CLIENTE-GET: Abortando GET de", nombreArchivo)
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

			fmt.Printf("Bloque %s recibido (%d bytes)\n", info.Block, len(data))

			// 4. Guardar en la posición correcta del slice
			bloquesRecuperados[indice] = data
		}()
	}

	err := reconstruirArchivo(nombreArchivo, bloquesRecuperados)
	if err != nil {
		fmt.Println("Error guardando archivo:", err)
	} else {
		fmt.Println("Archivo guardado con éxito")
	}

}

// Reconstruye y guarda el archivo solicitado
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
	outputPath := fmt.Sprintf("./%s", nombreArchivo)

	err := os.WriteFile(outputPath, final, 0644)
	if err != nil {
		return err
	}

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
		fmt.Println("Error al conectar con name_node", name_node_socket)
		return nil
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
