package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type Station struct {
	Name  []byte
	Sum   int
	Min   int
	Max   int
	Count int
}

var WorkerPool = runtime.NumCPU()

func main() {
	// // Start profiling
	// f, err := os.Create("cpu1brc.prof")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer pprof.StopCPUProfile()
	//
	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }

	doneChan := make(chan bool)

	f, ferr := os.Open("data/data.txt")
	if ferr != nil {
		fmt.Println("Can't read file ATM!", ferr)
		return
	}
	defer f.Close()

	jobs := make(chan []byte, WorkerPool)
	// results := make(chan map[uint32]*Station, WorkerPool)

	// Fan-out
	chunkWorkerChannels := make([]<-chan map[uint32]*Station, WorkerPool)
	for i := 0; i < WorkerPool; i++ {
		chunkWorkerChannels[i] = chunkWorker(jobs)
	}

	// Fan-in
	fanInStream := fanIn(doneChan, chunkWorkerChannels...)

	allStations := make([]map[uint32]*Station, 0, 1000)
	go func() {
		for result := range fanInStream {
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

		if eof != nil {
			log.Fatal(eof)
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

func chunkWorker(jobs <-chan []byte) <-chan map[uint32]*Station {
	results := make(chan map[uint32]*Station)

	go func() {
		defer close(results)
		for chunk := range jobs {
			results <- workOnChunk(chunk)
		}
	}()

	return results
}

func workOnChunk(buf []byte) map[uint32]*Station {
	var (
		h        = fnv.New32a()
		nameBuf  = make([]byte, 32)
		tempBuf  = make([]byte, 6)
		stations = make(map[uint32]*Station)
		cursor   = 0
	)

	for cursor < len(buf) {
		nxCursor, ns, ts := parseLine(cursor, buf, nameBuf, tempBuf)
		cursor = nxCursor
		// i := 0
		// for ln[i] != 59 {
		// 	i++
		// }
		name := nameBuf[:ns]
		temp := atof(tempBuf[:ts])

		h.Reset()
		h.Write(nameBuf[:ns])
		id := h.Sum32()
		station, ok := stations[id]
		if !ok {
			stations[id] = &Station{
				Name:  name,
				Min:   temp,
				Max:   temp,
				Count: 1,
				Sum:   temp,
			}
		} else {
			if temp < station.Min {
				station.Min = temp
			}
			if temp > station.Max {
				station.Max = temp
			}
			station.Sum += temp
			station.Count++
		}
	}

	return stations
}

func parseLine(cursor int, buf, nbuf, tbuf []byte) (int, int, int) {
	i := cursor
	ns := 0
	for buf[i] != 59 {
		nbuf[ns] = buf[i]
		i++
		ns++
	}

	i++ // skip semicolon
	ts := 0
	for i < len(buf) && buf[i] != 10 {
		tbuf[ts] = buf[i]
		i++
		ts++
	}

	i++
	return i, ns, ts
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

func fanIn(done <-chan bool, channels ...<-chan map[uint32]*Station) <-chan map[uint32]*Station {
	var wg sync.WaitGroup
	fannedInStream := make(chan map[uint32]*Station)

	transfer := func(c <-chan map[uint32]*Station) {
		defer wg.Done()
		for n := range c {
			select {
			case fannedInStream <- n:
			case <-done:
				return
			}
		}
	}

	for _, c := range channels {
		wg.Add(1)
		go transfer(c)
	}

	go func() {
		wg.Wait()
		close(fannedInStream)
	}()

	return fannedInStream
}
