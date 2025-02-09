package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
	"slices"
	"sort"
)

type Data struct {
	min   int
	max   int
	total int
	count int
	city  *[]byte
}

func multi(file_name string) {
	file, _ := os.Open(file_name)
	fi, _ := file.Stat()
	total_size := int(fi.Size())
	max_workers := runtime.NumCPU()
	interval := total_size / max_workers
	channel := make(chan map[uint64]*Data, max_workers)
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
	cities := make(map[uint64]*Data, 10000)
	for i := 0; i < max_workers; i++ {
		for city, temps := range <-channel {
			if existing, exists := cities[city]; exists {
				if temps.min < existing.min {
					existing.min = temps.min
				}
				if temps.max > existing.max {
					existing.max = temps.max
				}
				existing.total += temps.total
				existing.count += temps.count
			} else {
				cities[city] = temps
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

func parseInt(byteArrayPtr *[]byte, s int) int {
	byteArray := *byteArrayPtr
	if byteArray[s] == 45 {
		if byteArray[s+2] == 46 {
			return -(int(byteArray[s+1])*10 + int(byteArray[s+3]) - 528)
		}
		return -(int(byteArray[s+1])*100 + int(byteArray[s+2])*10 + int(byteArray[s+4]) - 5328)
	}
	if byteArray[s+1] == 46 {
		return int(byteArray[s])*10 + int(byteArray[s+2]) - 528
	}
	return int(byteArray[s])*100 + int(byteArray[s+1])*10 + int(byteArray[s+3]) - 5328
}

func FnvHash(b *[]byte) uint64 {
	h := fnv.New64a()
	h.Write(*b)
	return h.Sum64()
}

func parseRows(file_name string, start int, end int, ch chan<- map[uint64]*Data) {
	file, _ := os.Open(file_name)
	defer file.Close()
	file.Seek(int64(start), 0)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 1024*1024*64) // 64 MB buffer
	scanner.Buffer(buf, len(buf))
	cities := make(map[uint64]*Data, 10000)
	for scanner.Scan() {
		byteArray := scanner.Bytes()
		start += len(byteArray) + 1

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
		if start >= end {
			break
		}
	}
	ch <- cities
}
