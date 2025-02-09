package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func single(file_name string) {
	file, _ := os.Open(file_name)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	cities := make(map[string]*[4]float64)

	for scanner.Scan() {
		line := strings.Split(scanner.Text(), ";")
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
