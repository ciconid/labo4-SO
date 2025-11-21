package main

import (
    "fmt"
    "io"
    "os"
)

func main() {
    filePath := "lotr.txt"

    file, err := os.Open(filePath)
    if err != nil {
        panic(err)
    }
    defer file.Close()

    const blockSize = 1024 // 1 KB
    buffer := make([]byte, blockSize)

    var blocks [][]byte // <- slice donde guardaremos todos los bloques

    for {
        n, err := file.Read(buffer)
        if err != nil {
            if err == io.EOF {
                break
            }
            panic(err)
        }

        // Copiar el bloque real en un nuevo slice
        block := make([]byte, n)
        copy(block, buffer[:n])

        blocks = append(blocks, block)
    }

    fmt.Println("Cantidad de bloques:", len(blocks))

    // Ejemplo: mostrar tamaño de los primeros bloques
    for i, blk := range blocks {
        fmt.Printf("Bloque %d → %d bytes\n", i, len(blk))
    }
}
