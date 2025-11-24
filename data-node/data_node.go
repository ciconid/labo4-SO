// Ejecutar desde ./data-node/

package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	logYPrint("Escuchando en puerto 9000...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			msg := fmt.Sprintf("Error aceptando conexión: %s", err)
			logYPrint(msg)
			continue
		}

		msg := fmt.Sprintf("Cliente conectado desde: %s", conn.RemoteAddr())
		logYPrint(msg)

		go handle(conn)
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
			partesArgumento := strings.SplitN(argumento, " ", 2)
			nombreArchivo := partesArgumento[0]
			contenidoArchivo := partesArgumento[1]

			msg := fmt.Sprintf("STORE de %s", nombreArchivo)
			logYPrint(msg)

			writePath := fmt.Sprintf("./blocks/%s", nombreArchivo)

			err := os.WriteFile(writePath, []byte(contenidoArchivo), 0644)
			if err != nil {
				msg := fmt.Sprintf("Error al escribir archivos %s", err)
				logYPrint(msg)
			}
		case "READ":
			nombreArchivo := argumento

			msg := fmt.Sprintf("READ de %s", nombreArchivo)
			logYPrint(msg)

			// Ruta del archivo a leer
			readPath := fmt.Sprintf("./blocks/%s", nombreArchivo)

			// Leer todo el contenido del archivo
			data, err := os.ReadFile(readPath)
			if err != nil {
				msg := fmt.Sprintf("Error al leer archivo: %s", err)
				logYPrint(msg)
				conn.Write([]byte("ERROR al leer archivo")) // respuesta mínima
				return
			}

			// Enviar el archivo al cliente (solo los bytes)
			_, err = conn.Write(data)
			if err != nil {
				msg := fmt.Sprintf("Error enviando archivo: %s", err)
				logYPrint(msg)
				return
			}
	}
}

func logYPrint(msg string) {
	// Abrir archivo en modo append
	f, err := os.OpenFile("./logs/data-node.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	f, err := os.OpenFile("./logs/data-node.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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