package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	dimension := 4
	k := 5
	numOfThreads := 7
	minChange := 0.00000000001

	f, _ := os.Open("C:\\data\\fakeDataBig.csv") //TODO create folder at this location or change path to your dataset
	points := make([]Point, 0)
	// Create a new reader.
	r := csv.NewReader(bufio.NewReader(f))
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}
		point := Point{
			make([]float64, 0),
			dimension,
		}
		for value := range record {
			num, _ := strconv.ParseFloat(record[value], 64)
			point.numbers = append(point.numbers, num)
		}
		points = append(points, point)
	}
	kMeans := Kmeans{
		points,
		k,
		dimension,
		minChange,
		numOfThreads,
	}
	start := time.Now()
	clusters := kMeans.doKmeansSerial()
	elapsed := time.Since(start)
	log.Printf("Kmeans serial took %s", elapsed)
	//Check if all clusters were used
	sumLen := 0
	for _, value := range clusters {
		sumLen += len(value)
		println(value)
	}
	//Check if all points are present
	println(sumLen)
	start = time.Now()
	clusters = kMeans.doKmeansParallel()
	elapsed = time.Since(start)
	log.Printf("Kmeans parallel took %s", elapsed)
	sumLen = 0
	for _, value := range clusters {
		sumLen += len(value)
		println(value)
	}
	println(sumLen)
}
