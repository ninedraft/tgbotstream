package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	n := 2
	flag.IntVar(&n, "n", n, "number of parts")

	flag.Parse()

	n = max(1, n)

	defer os.Stdout.WriteString("\n")

	fmt.Printf("user:%s", generate(n))
}

func generate(n int) string {
	parts := make([]string, n)

	for i := range parts {
		parts[i] = rand.Text()
	}

	return strings.Join(parts, "")
}
