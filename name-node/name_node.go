package main

import (
	"bufio"
	"encoding/json"
	"fmt"
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

var data_node_sockets = []string{
	"192.168.100.174:9000",
	"192.168.100.97:9000",
}

func main() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	log("Escuchando en puerto 9000...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error aceptando conexión:", err)
			msg := fmt.Sprintf("Error aceptando conexión: %s", err)
			log(msg)
			continue
		}

		msg := fmt.Sprintf("Cliente conectado desde: %s", conn.RemoteAddr())
		log(msg)

		go handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	// Leer comando del buffer
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	// comando := string(buf[:n])
	input := string(buf[:n])
	line := strings.TrimSpace(input)
	partes := strings.SplitN(line, " ", 2)

	comando := partes[0]
	argumento := ""
	if len(partes) > 1 {
		argumento = partes[1]
	}

	switch comando {
	case "LISTAR":
		log("Ejecutando LISTAR")
		listaDeArchivos := crearListaArchivos()

		jsonData, _ := json.Marshal(listaDeArchivos)
		conn.Write(jsonData)
	case "INFO":
		log("Ejecutando INFO")
		infoArchivo, err := obtenerInfoArchivo(argumento)
		if err != nil {
			msg := fmt.Sprintf("Error infoArchivo: %s", err)
			log(msg)
			return
		}

		jsonData, _ := json.Marshal(infoArchivo)
		conn.Write(jsonData)
	case "PUT":
		log("Ejecutando PUT")
		nombreArchivo, cantBloques, err := parseArgumento(argumento)
		if err != nil {
			msg := fmt.Sprintf("Error PUT: %s", err)
			log(msg)
			return
		}

		cantDataNodes := len(data_node_sockets)
		dataNodeIndex := 0

		var asignaciones []BlockInfo

		for bloque := 1; bloque <= cantBloques; bloque++ {
			fmt.Println(bloque, data_node_sockets[dataNodeIndex])
			asignaciones = append(asignaciones, BlockInfo{
				Block: strconv.Itoa(bloque),
				Node:  data_node_sockets[dataNodeIndex],
			})

			dataNodeIndex++
			if dataNodeIndex == cantDataNodes {
				dataNodeIndex = 0
			}
		}

		jsonData, _ := json.Marshal((asignaciones))
		conn.Write(jsonData)

		// esperar confirmacion de transferencia completa
		reader := bufio.NewReader(conn)
		msg, err := reader.ReadString('\n')
		if err != nil {
			msg := fmt.Sprintf("Error leyendo %s", err)
			log(msg)
			return
		}

		if strings.TrimSpace(msg) == "TRANSFER_COMPLETE" {
			log("Cliente termino la transferencia con exito")
			err = actualizarMetadata(nombreArchivo, asignaciones)
			if err != nil {
				log("Error al actualizar metadata")
			}
		}
	}
}

func parseArgumento(argumento string) (string, int, error) {
	partes := strings.SplitN(argumento, " ", 2)
	if len(partes) != 2 {
		return "", 0, fmt.Errorf("argumento inválido: '%s'", argumento)
	}

	nombreArchivo := partes[0]
	cantBloquesStr := partes[1]

	cantBloquesNum, err := strconv.Atoi(cantBloquesStr)
	if err != nil {
		return "", 0, err
	}

	return nombreArchivo, cantBloquesNum, nil
}

func obtenerInfoArchivo(nombreArchivo string) ([]BlockInfo, error) {
	metadata, err := cargarMetadata()
	if err != nil {
		return nil, err
	}

	info, existe := metadata[nombreArchivo]
	if !existe {
		return nil, fmt.Errorf("el archivo %s no existe en metadata.json", nombreArchivo)
	}

	return info, nil
}

func crearListaArchivos() []string {
	metadata, err := cargarMetadata()
	if err != nil {
		msg := fmt.Sprintf("Error al cargar metadata %s", err)
		log(msg)
		return nil
	}

	var listaDeArchivos []string
	for nombre := range metadata {
		listaDeArchivos = append(listaDeArchivos, nombre)
	}

	return listaDeArchivos
}

func cargarMetadata() (map[string][]BlockInfo, error) {
	data, err := os.ReadFile("./metadata.json")
	if err != nil {
		return nil, err
	}

	var metadata map[string][]BlockInfo

	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func actualizarMetadata(fileName string, nuevosBloques []BlockInfo) error {
	// 1) Cargar metadata existente
	metadata, err := cargarMetadata()
	if err != nil {
		return err
	}

	// 2) Reemplazar o crear la entrada del archivo
	metadata[fileName] = nuevosBloques

	// 3) Guardar en metadata.json
	return guardarMetadata(metadata)
}

func guardarMetadata(metadata map[string][]BlockInfo) error {
	const metadataPath = "./metadata.json"

	output, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(metadataPath, output, 0644)
}

func log(msg string) {
	// Abrir archivo en modo append
	f, err := os.OpenFile("./logs/name-node.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
