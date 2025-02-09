package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"sort"
)

func multi(file_name string) {
	file, _ := os.Open(file_name)
	fi, _ := file.Stat()
	total_size := int(fi.Size())
	max_workers := runtime.NumCPU()
	interval := total_size / max_workers
	channel := make(chan map[string]*[4]float64, max_workers)
	start := 0
	for i := 1; i <= max_workers; i++ {
		end := i * interval
		if i < max_workers {
			file.Seek(int64(end), 0)
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
	cities := make(map[string]*[4]float64, 10000)
	for i := 0; i < max_workers; i++ {
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
	final_cities := make([]string, 0, len(cities))
	for city, temps := range cities {
		final_cities = append(final_cities, fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, temps[0]/10, temps[1]/(temps[3]*10), temps[2]/10))
	}
	sort.Strings(final_cities)
	for _, value := range final_cities {
		fmt.Println(value)
	}
}

func parseFloat(byteArrayPtr *[]byte, s int) float64 {
	byteArray := *byteArrayPtr
	if byteArray[s] == 45 {
		if byteArray[s+2] == 46 {
			return -(float64(byteArray[s+1])*10 + float64(byteArray[s+3]) - 528)
		}
		return -(float64(byteArray[s+1])*100 + float64(byteArray[s+2])*10 + float64(byteArray[s+4]) - 5328)
	}
	if byteArray[s+1] == 46 {
		return float64(byteArray[s])*10 + float64(byteArray[s+2]) - 528
	}
	return float64(byteArray[s])*100 + float64(byteArray[s+1])*10 + float64(byteArray[s+3]) - 5328
}

func parseRows(file_name string, start int, end int, ch chan<- map[string]*[4]float64) {
	file, _ := os.Open(file_name)
	defer file.Close()
	file.Seek(int64(start), 0)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 1024*1024*64) // 64 MB buffer
	scanner.Buffer(buf, len(buf))
	cities := make(map[string]*[4]float64, 10000)
	var city string
	var temp float64
	for scanner.Scan() {
		byteArray := scanner.Bytes()
		start += len(byteArray) + 1

		s := slices.Index(byteArray, 59)
		city = string(byteArray[:s])
		temp = parseFloat(&byteArray, s+1)

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
