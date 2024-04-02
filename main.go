package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
)

type Station struct {
	name  string
	temps []float64
	min   float64
	avg   float64
	max   float64
}

func main() {
	// Start profiling
	f, err := os.Create("cpu1brc.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}

	// Run your program here
	// _, err = parseWithScan("data.txt")
	_, err = parseWithBuffer("data/data.txt")
	if err != nil {
		fmt.Println("Can't read file ATM!", err)
	}

	fmt.Println("Done!")
}

func calcTemps(temps []float64) (avg, min, max float64) {
	min = temps[0]
	max = temps[0]
	sum := 0.0

	for i, v := range temps {
		if v < min {
			min = temps[i]
		} else if v > max {
			max = temps[i]
		}

		sum += v
	}

	return sum / float64(len(temps)), min, max
}

func parseWithBuffer(filename string) (int, error) {
	f, ferr := os.Open(filename)
	if ferr != nil {
		return 0, ferr
	}
	defer f.Close()

	var (
		buf      = make([]byte, 4*1024*1024)
		counter  = 0
		stations = make(map[uint32][]int)
		reminder = make([]byte, 0, 1024*10)
	)

	h := fnv.New32a()

Outer:
	for {
		n, eof := f.Read(buf)
		// fmt.Println("Read", n, "bytes")

		startAt, endAt := getBufIndexes(buf, n)

		r := bytes.NewReader(buf[startAt:endAt])
		scanner := bufio.NewScanner(r)

		for k := 0; k < startAt-1; k++ {
			reminder = append(reminder, buf[k])
		}

		for k := endAt; k < n; k++ {
			reminder = append(reminder, buf[k])
		}

		for scanner.Scan() {
			ln := scanner.Bytes()
			idx := indexOf(ln)

			if idx == -1 {
				fmt.Println("Errr line ->", string(ln))
				continue
			}

			name := ln[:idx]
			temp := ln[idx+1:]
			if len(name) == 0 || len(temp) == 0 {
				fmt.Println("Error", "name or temp len 0!")
				reminder = concatBytes(reminder, ln)
				continue
			}
			h.Reset()
			h.Write(name)
			id := h.Sum32()
			// ftemp, err := strconv.ParseFloat(string(temp), 64)
			// if err != nil {
			// 	fmt.Println("Error", err)
			// }

			stations[id] = append(stations[id], atof(temp))

			counter++
			if counter > 100_000_000 {
				break Outer
			}
		}

		if eof != nil {
			if eof != io.EOF {
				log.Fatal(eof)
			}
			break Outer
		}
	}

	// fmt.Println("Reminder", string(reminder))

	// os.Stdout.Write(buf[:n])
	// var avg, min, max float64
	// for k, v := range stations {
	// 	avg, min, max = calcTemps(v.temps)
	// 	fmt.Printf("%v:%.1f/%.1f/%.1f\n", k, min, avg, max)
	// }

	return counter, nil
}

func parseChunkWithScan(buf []byte, stations *map[uint32]Station, outCounter uint) (uint, []byte) {
	sts := *stations
	r := bytes.NewReader(buf)
	scanner := bufio.NewScanner(r)
	h := fnv.New32a()
	counter := outCounter

	for scanner.Scan() {
		ln := scanner.Bytes()

		idx := indexOf(ln)

		if idx == -1 {
			return counter, ln
		}

		h.Write(ln[:idx])

		tempF, err := strconv.ParseFloat(string(ln[idx+1:]), 64)
		if err != nil {
			fmt.Println("Error", err)
		}

		counter++
		station := sts[h.Sum32()]
		station.temps = append(sts[h.Sum32()].temps, tempF)
		sts[h.Sum32()] = station
	}

	return counter, nil
}

func indexOf(ln []byte) int {
	idx := -1
	i := 0

	for i < len(ln) && ln[i] != 59 {
		i++
	}

	if i < len(ln) {
		idx = i
	}

	return idx
}

func atof(bArray []byte) int {
	neg := false
	res := 0
	for i := 0; i < len(bArray); i++ {
		if bArray[i] == 45 {
			neg = true
			continue
		}

		if bArray[i] == 46 {
			continue
		}
		res = res*10 + int(bArray[i]-48)
	}

	if neg {
		return -res
	}

	return res
}

func concatBytes(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}

func testStruct() {
	st := Station{
		name: "Test",
		temps: []float64{
			1.2,
			2.3,
			3.4,
		},
	}

	st.avg = 2.3
	fmt.Println("Station", st)
}

func getBufIndexes(buf []byte, n int) (int, int) {
	startAt := 0
	for startAt < n {
		if buf[startAt] == 10 {
			// fmt.Println("Found new line at begining ->", startAt)
			break
		}
		startAt++
	}

	endAt := n - 1
	for endAt > 0 {
		if buf[endAt] == 10 {
			// fmt.Println("Found new line at end", endAt)
			break
		}
		endAt--
	}
	return startAt + 1, endAt
}
