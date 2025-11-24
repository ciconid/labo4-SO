// Ejecutar desde la raiz (labo4-SO)

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

var name_node_socket = "192.168.100.103:9000"

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Comandos disponibles:")
	fmt.Println("  ls                → listar archivos")
	fmt.Println("  put <archivo>     → subir archivo")
	fmt.Println("  get <archivo>     → descargar archivo")
	fmt.Println("  info <archivo>    → ver metadata")
	fmt.Println("  cat <archivo>    → cat remoto")
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
			logOnly("Ejecutando ls")
			if argumento != "" {
				logYPrint("Uso incorrecto del comando")
				fmt.Println("Uso: > ls")
				break
			}
			ls()
		case "put":
			logOnly("Ejecutando put")
			if argumento == "" {
				logYPrint("Uso incorrecto del comando")
				fmt.Println("Uso: > put <archivo>")
				break
			}
			put(argumento)
		case "get":
			logOnly("Ejecutando get")
			if argumento == "" {
				logYPrint("Uso incorrecto del comando")
				fmt.Println("Uso: > get <archivo>")
				break
			}
			get(argumento)
		case "info":
			logOnly("Ejecutando info")
			if argumento == "" {
				logYPrint("Uso incorrecto del comando")
				fmt.Println("Uso: > info <archivo>")
				break
			}
			info(argumento)
		case "cat":
			logOnly("Ejecutando cat")
			if argumento == "" {
				logYPrint("Uso incorrecto del comando")
				fmt.Println("Uso: > cat <archivo>")
				break
			}
			cat(argumento)
		case "help":
			logOnly("Ejecutando help")
			if argumento != "" {
				logYPrint("Uso incorrecto del comando")
				fmt.Println("Uso: > help")
				break
			}
			fmt.Println("Comandos disponibles:")
			fmt.Println("  ls                → listar archivos")
			fmt.Println("  put <archivo>     → subir archivo")
			fmt.Println("  get <archivo>     → descargar archivo")
			fmt.Println("  info <archivo>    → ver metadata")
			fmt.Println("  cat <archivo>    → cat remoto")
			fmt.Println("  help              → mostrar esta ayuda")
		default:
			logYPrint("Comando inválido")
		}
	}
}

func ls() {
	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		msg := fmt.Sprintf("Error al conectar con name_node %s", name_node_socket)
		logYPrint(msg)
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

	msg := fmt.Sprintf("Iniciando PUT de %s", nombreArchivo)
	logYPrint(msg)

	bloques, err := LeeArchivoEnBloques(nombreArchivo, tamanioBloque)
	if err != nil {
		msg := fmt.Sprintf("CLIENTE-PUT: Error al leer archivo %s", nombreArchivo)
		logYPrint(msg)
		msg = fmt.Sprintf("Abortando PUT de %s", nombreArchivo)
		logYPrint(msg)
		return
	}
	cantBloques := len(bloques)

	conn, err := net.Dial("tcp", name_node_socket)
	if err != nil {
		msg := fmt.Sprintf("Error al conectar con name_node %s", name_node_socket)
		logYPrint(msg)
		return
	}
	defer conn.Close()

	// Enviar comando
	comando := fmt.Sprintf("PUT %s %d", argumento, cantBloques)
	conn.Write([]byte(comando))

	msg = fmt.Sprintf("  Enviando comando %s a name_node %s", comando, name_node_socket)
	logOnly(msg)

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
				msg := fmt.Sprintf("CLIENTE-PUT: Error conexion con el nodo %s", item.Node)
				logYPrint(msg)
				msg = fmt.Sprintf("Abortando PUT de %s", nombreArchivo)
				logYPrint(msg)
				abort = true
				return
			}
			defer conn.Close()

			// Enviar comando
			blockIndex, err := strconv.Atoi(item.Block)
			if err != nil {
				logYPrint("Error al convertir de string a int")
				abort = true
				return
			}
			comando := fmt.Sprintf("STORE b%s_%s %s", item.Block, nombreArchivo, bloques[blockIndex-1])
			conn.Write([]byte(comando))

			msg := fmt.Sprintf("  Subiendo bloque %s a %s", item.Block, item.Node)
			logYPrint(msg)
		}()

		if abort {
			break
		}
	}

	if abort {
		return
	}

	// avisar a NameNode que PUT fue exitoso (para poder guardar la info en metadata.json)
	msg = "TRANSFER_COMPLETE\n"
	_, err = conn.Write([]byte(msg))
	if err != nil {
		logYPrint("CLIENTE-PUT: Error al enviar TRANSFER_COMPLETE al name_node")
		return
	}
	msg = fmt.Sprintf("Transferencia de %s completa", nombreArchivo)
	logYPrint(msg)
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
	abort := false

	var lista []BlockInfo = recuperarInfoDeArchivo(nombreArchivo)

	if lista == nil {
		msg := fmt.Sprintf("CLIENTE-GET: El archivo %s no existe", nombreArchivo)
		logYPrint(msg)
		return
	}

	for _, item := range lista {
		msg := fmt.Sprintf("Block: %s - Node: %s", item.Block, item.Node)
		logYPrint(msg)
	}
	fmt.Println()

	// Necesitamos saber cuántos bloques habrá para crear el slice final
	bloquesRecuperados := make([][]byte, len(lista))

	for _, info := range lista {
		// Convertir el Block (string) → índice int
		numBloque, err := strconv.Atoi(info.Block)
		if err != nil {
			msg := fmt.Sprintf("Block inválido: %s", info.Block)
			logYPrint(msg)
			continue
		}
		indice := numBloque - 1 // bloque 1 → índice 0

		// 1. Conectar al DataNode
		timeout := 2 * time.Second
		conn, err := net.DialTimeout("tcp", info.Node, timeout)
		if err != nil {
			msg := fmt.Sprintf("Error conectando a %s: %s", info.Node, err)
			logYPrint(msg)
			msg = fmt.Sprintf("CLIENTE-GET: Abortando GET de %s", nombreArchivo)
			logYPrint(msg)
			return
		}

		func() {
			defer conn.Close()

			// 2. Solicitar el bloque
			req := fmt.Sprintf("READ b%s_%s\n", info.Block, nombreArchivo)
			_, err = conn.Write([]byte(req))
			if err != nil {
				msg := fmt.Sprintf("Error enviando solicitud: %s", err)
				logYPrint(msg)
				abort = true
				return
			}

			// 3. Leer el bloque completo
			data, err := io.ReadAll(conn)
			if err != nil {
				msg := fmt.Sprintf("Error leyendo bloque %s: %s", info.Block, err)
				logYPrint(msg)
				abort = true
				return
			}

			if len(data) == 21 {
				s := string(data)
				if s == "ERROR al leer archivo" {
					msg := fmt.Sprintf("%s %s", s, nombreArchivo)
					logYPrint(msg)
					msg = fmt.Sprintf("CLIENTE-GET: Abortando GET de %s", nombreArchivo)
					logYPrint(msg)
					abort = true
					return
				}
			}

			// fmt.Println(data)
			msg := fmt.Sprintf("  Bloque %s recibido (%d bytes)", info.Block, len(data))
			logYPrint(msg)

			// 4. Guardar en la posición correcta del slice
			bloquesRecuperados[indice] = data
		}()

		if abort {
			break
		}
	}

	if abort {
		return
	}

	err := reconstruirArchivo(nombreArchivo, bloquesRecuperados)
	if err != nil {
		msg := fmt.Sprintf("Error guardando archivo: %s", err)
		logYPrint(msg)
	} else {
		logYPrint("Archivo guardado con éxito")
	}

}

func cat(nombreArchivo string) {
	abort := false

	// fmt.Println("Hola desde cat")
	var lista []BlockInfo = recuperarInfoDeArchivo(nombreArchivo)

	if lista == nil {
		msg := fmt.Sprintf("CLIENTE-CAT: El archivo %s no existe", nombreArchivo)
		logYPrint(msg)
		return
	}

	// Necesitamos saber cuántos bloques habrá para crear el slice final
	bloquesRecuperados := make([][]byte, len(lista))

	for _, info := range lista {
		// Convertir el Block (string) → índice int
		numBloque, err := strconv.Atoi(info.Block)
		if err != nil {
			msg := fmt.Sprintf("Block inválido: %s", info.Block)
			logYPrint(msg)
			continue
		}
		indice := numBloque - 1 // bloque 1 → índice 0

		// 1. Conectar al DataNode
		timeout := 2 * time.Second
		conn, err := net.DialTimeout("tcp", info.Node, timeout)
		if err != nil {
			msg := fmt.Sprintf("Error conectando a %s: %s", info.Node, err)
			logYPrint(msg)
			msg = fmt.Sprintf("CLIENTE-CAT: Abortando CAT de %s", nombreArchivo)
			logYPrint(msg)
			continue
		}

		func() {
			defer conn.Close()

			// 2. Solicitar el bloque
			req := fmt.Sprintf("READ b%s_%s\n", info.Block, nombreArchivo)
			_, err = conn.Write([]byte(req))
			if err != nil {
				msg := fmt.Sprintf("Error enviando solicitud: %s", err)
				logYPrint(msg)
				abort = true
				return
			}

			// 3. Leer el bloque completo
			data, err := io.ReadAll(conn)
			if err != nil {
				msg := fmt.Sprintf("Error leyendo bloque %s: %s", info.Block, err)
				logYPrint(msg)
				abort = true
				return
			}

			if len(data) == 21 {
				s := string(data)
				if s == "ERROR al leer archivo" {
					msg := fmt.Sprintf("%s %s", s, nombreArchivo)
					logYPrint(msg)
					msg = fmt.Sprintf("CLIENTE-CAT: Abortando CAT de %s", nombreArchivo)
					logYPrint(msg)
					abort = true
					return
				}
			}

			// 4. Guardar en la posición correcta del slice
			bloquesRecuperados[indice] = data
		}()

		if abort {
			break
		}
	}

	if abort {
		return
	}

	fmt.Println(bytes2String(bloquesRecuperados))
}

func bytes2String(blocks [][]byte) string {
	// Calculamos el tamaño total para evitar realocaciones
	total := 0
	for _, b := range blocks {
		total += len(b)
	}

	// Creamos un slice de bytes de tamaño final
	final := make([]byte, 0, total)

	// Concatenamos usando append(..., bloque...)
	for _, bloque := range blocks {
		final = append(final, bloque...)
	}

	// Convertimos el []byte a string
	return string(final)
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

	if lista == nil {
		msg := fmt.Sprintf("El archivo %s no existe", argumento)
		logYPrint(msg)
		return
	}

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

func logYPrint(msg string) {
	// Abrir archivo en modo append
	f, err := os.OpenFile("./cliente/logs/cliente.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error al abrir log:", err)
		return
	}
	defer f.Close()

	// Timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	// Línea con timestamp
	logLine := fmt.Sprintf("[%s] %s\n", timestamp, msg)

	// Escribir al archivo e imprimir por consola
	f.WriteString(logLine)
	fmt.Println(msg)
}

func logOnly(msg string) {
	// Abrir archivo en modo append
	f, err := os.OpenFile("./cliente/logs/cliente.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error al abrir log:", err)
		return
	}
	defer f.Close()

	// Timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	// Línea con timestamp
	logLine := fmt.Sprintf("[%s] %s\n", timestamp, msg)

	// Escribir al archivo e imprimir por consola
	f.WriteString(logLine)
}
