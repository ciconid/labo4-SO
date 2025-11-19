package main

import (
	"fmt"
	"net"
	"encoding/json"
	"os"
	"strings"
)

type BlockInfo struct {
	Block string `json:"block"`
	Node string `json:"node"`
}

//var listaDeArchivos = []string{"archivo1.txt", "archivo2.txt"} // esta lista la tiene que generar el propio name_node

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
		argumento = partes [1]
	}

	switch comando {
		case "LISTAR":
			listaDeArchivos := crearListaArchivos()

			// Serializar a JSON
			jsonData, _ := json.Marshal(listaDeArchivos)
			conn.Write(jsonData)
		case "INFO":
			infoArchivo, err := obtenerInfoArchivo(argumento) 
			if err != nil {
				fmt.Errorf("Error infoArchivo: %s", err)
				return
			}

			jsonData, _ := json.Marshal(infoArchivo)
			conn.Write(jsonData)

	}

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
		fmt.Println("Error al cargar metadata", err)
		return nil
	}
	
	var listaDeArchivos []string
	for nombre := range metadata {
		listaDeArchivos = append(listaDeArchivos, nombre)
	}

	// fmt.Println(listaDeArchivos)
	return listaDeArchivos
}

func cargarMetadata() (map[string][]BlockInfo, error) {
	data, err := os.ReadFile("metadata.json")
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