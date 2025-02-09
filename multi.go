package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

func multi(file_name string) {
	file, _ := os.Open(file_name)
	fi, _ := file.Stat()
	total_size := fi.Size()
	max_workers := int64(runtime.NumCPU())
	interval := total_size / max_workers
	channel := make(chan map[string]*[4]float64, max_workers)
	start := int64(0)
	for i := int64(1); i <= max_workers; i++ {
		end := i * interval
		if i < max_workers {
			file.Seek(end, 0)
			b := make([]byte, 1)
			for b[0] != '\n' {
				file.Read(b)
				end++
			}
		} else {
			end = total_size
		}
		go parseRows(file_name, start, end, channel)
		start = end
	}
	cities := make(map[string]*[4]float64)
	for i := int64(0); i < max_workers; i++ {
		for city, temps := range <-channel {
			if existing, exists := cities[city]; exists {
				existing[0] = math.Min(existing[0], temps[0])
				existing[1] += temps[1]
				existing[2] = math.Max(existing[2], temps[2])
				existing[3] += temps[3]
			} else {
				cities[city] = temps
			}
		}
	}
	// Abha=-23.0/18.0/59.2
	var final_cities []string
	for city, temps := range cities {
		final_cities = append(final_cities, fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, temps[0], temps[1]/temps[3], temps[2]))
	}
	sort.Strings(final_cities)
	for _, value := range final_cities {
		fmt.Println(value)
	}
}

func parseRows(file_name string, start int64, end int64, ch chan<- map[string]*[4]float64) {
	file, _ := os.Open(file_name)
	defer file.Close()
	file.Seek(start, 0)
	scanner := bufio.NewScanner(file)
	cities := make(map[string]*[4]float64)
	for scanner.Scan() {
		text := scanner.Text()
		start += int64(len(text) + 1)
		line := strings.Split(text, ";")
		city := line[0]
		temp, _ := strconv.ParseFloat(line[1], 64)
		if existing, exists := cities[city]; exists {
			existing[0] = math.Min(existing[0], temp)
			existing[1] += temp
			existing[2] = math.Max(existing[2], temp)
			existing[3]++
		} else {
			cities[city] = &[4]float64{temp, temp, temp, 1}
		}
		if start >= end {
			break
		}
	}
	ch <- cities
}
