package main

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"sort"
)

func single(file_name string) {
	file, _ := os.Open(file_name)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 1024*1024*64) // 64 MB buffer
	scanner.Buffer(buf, len(buf))

	cities := make(map[uint64]*Data, 10000)

	for scanner.Scan() {
		byteArray := scanner.Bytes()

		s := slices.Index(byteArray, 59)
		cityBytes := byteArray[:s]
		city := FnvHash(&cityBytes)
		temp := parseInt(&byteArray, s+1)

		if existing, exists := cities[city]; exists {
			if temp < existing.min {
				existing.min = temp
			} else if temp > existing.max {
				existing.max = temp
			}
			existing.total += temp
			existing.count++
		} else {
			cityBytesCopy := make([]byte, len(cityBytes))
			copy(cityBytesCopy, cityBytes)
			cities[city] = &Data{
				min:   temp,
				max:   temp,
				total: temp,
				count: 1,
				city:  &cityBytesCopy,
			}
		}
	}
	// Abha=-23.0/18.0/59.2
	final_cities := make([]string, 0, len(cities))
	for _, temps := range cities {
		final_cities = append(final_cities, fmt.Sprintf("%s=%.1f/%.1f/%.1f", string(*temps.city), float64(temps.min)/10, float64(temps.total)/float64(temps.count*10), float64(temps.max)/10))
	}
	sort.Strings(final_cities)
	for _, value := range final_cities {
		fmt.Println(value)
	}
}
