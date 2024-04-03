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
	"sync"
)

type Station struct {
	name  string
	temps []float64
	min   float64
	avg   float64
	max   float64
}

var WorkerPool = 16

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

	var wg sync.WaitGroup
	doneChan := make(chan bool)

	f, ferr := os.Open("data/data.txt")
	if ferr != nil {
		fmt.Println("Can't read file ATM!", ferr)
		return
	}
	defer f.Close()

	jobs := make(chan []byte, WorkerPool)
	results := make(chan map[uint32][]int, WorkerPool)

	wg.Add(WorkerPool)
	for w := 0; w < WorkerPool; w++ {
		go chunkWorker(jobs, results, &wg)
	}

	allStations := make([]map[uint32][]int, 0, 1000)
	go func() {
		for result := range results {
			allStations = append(allStations, result)
		}

		doneChan <- true
	}()

	var (
		buf          = make([]byte, 4*1024*1024)
		leftover     = make([]byte, 1024)
		leftoverSize = 0
		chunkCount   = 0
	)
	for {
		n, eof := f.Read(buf)
		if eof == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		k := 0
		for i := n - 1; i >= 0; i-- {
			if buf[i] == 10 {
				k = i
				break
			}
		}

		chunk := make([]byte, k+leftoverSize)
		copy(chunk, leftover[:leftoverSize])
		copy(chunk[leftoverSize:], buf[:k])
		copy(leftover, buf[k+1:n])
		// fmt.Println("initial -->", string(chunk[:20]))
		// fmt.Println("leftover -->", string(buf[k+1:n]))
		leftoverSize = n - k - 1

		jobs <- chunk // send work to chunkWorker

		chunkCount++
		// if chunkCount == 5 {
		// 	break
		// }
	}
	close(jobs)

	wg.Wait()
	close(results)

	<-doneChan
	fmt.Println("Chunk count", chunkCount)
	fmt.Println("All Work Done!")

	// fmt.Println("Reminder", string(leftover))
	// os.Stdout.Write(buf[:n])
	// var avg, min, max float64
	// for k, v := range stations {
	// 	avg, min, max = calcTemps(v.temps)
	// 	fmt.Printf("%v:%.1f/%.1f/%.1f\n", k, min, avg, max)
	// }
}

func chunkWorker(jobs <-chan []byte, results chan<- map[uint32][]int, wg *sync.WaitGroup) {
	defer wg.Done()

	for chunk := range jobs {
		results <- workOnChunk(chunk)
	}
}

func workOnChunk(buf []byte) map[uint32][]int {
	r := bytes.NewReader(buf)
	scanner := bufio.NewScanner(r)
	stations := make(map[uint32][]int)
	nameBuf := make([]byte, 50)
	tempBuf := make([]byte, 100)

	h := fnv.New32a()
	for scanner.Scan() {
		ln := scanner.Bytes()
		nameLen, tempLen := parseLine(ln, nameBuf, tempBuf)
		h.Reset()
		h.Write(nameBuf[:nameLen])
		id := h.Sum32()
		stations[id] = append(stations[id], atof(tempBuf[:tempLen]))
	}

	return stations
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

func parseLine(ln []byte, nameBuf []byte, tempBuf []byte) (int, int) {
	// idx := -1
	i := 0
	ns := 0
	for ln[i] != 59 {
		nameBuf[i] = ln[i]
		i++
		ns++
	}

	i++
	ts := 0
	for i < len(ln) && ln[i] != 10 {
		tempBuf[ts] = ln[i]
		i++
		ts++
	}

	return ns, ts
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
