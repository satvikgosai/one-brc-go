package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	arg_len := len(os.Args)
	args := os.Args
	if arg_len < 2 {
		fmt.Println("Please provide measurement file")
	} else {
		file, err := os.Open(args[1])
		defer file.Close()
		if err != nil {
			fmt.Printf("Error reading file: '%s'\n", args[1])
		} else {
			scanner := bufio.NewScanner(file)
			if scanner.Scan() {
				line := strings.Split(scanner.Text(), ";")
				_, err := strconv.ParseFloat(line[1], 64)
				if err != nil {
					fmt.Println("This file does not seems to be a measurements file")
				} else if arg_len > 2 && args[2] == "--single" {
					single(args[1])
				} else {
					multi(args[1])
				}
			}
		}
	}
}
