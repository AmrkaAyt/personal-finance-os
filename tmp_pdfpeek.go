package main

import (
  "fmt"
  "io"
  "os"
  pdf "github.com/ledongthuc/pdf"
)

func main() {
  for _, path := range os.Args[1:] {
    f, r, err := pdf.Open(path)
    if err != nil { fmt.Printf("FILE %s\nERR %v\n", path, err); continue }
    textReader, err := r.GetPlainText()
    if err != nil { fmt.Printf("FILE %s\nERR %v\n", path, err); _ = f.Close(); continue }
    data, err := io.ReadAll(io.LimitReader(textReader, 6000))
    if err != nil { fmt.Printf("FILE %s\nERR %v\n", path, err); _ = f.Close(); continue }
    _ = f.Close()
    fmt.Printf("FILE %s\n%s\n----\n", path, string(data))
  }
}
