package main

import (
	"math"
	"math/rand"
	"time"
)

type Point struct {
	numbers   []float64
	dimension int
}

type Kmeans struct {
	points       []Point
	k            int
	dimension    int
	minChange    float64
	numOfThreads int
}

type Cluster struct {
	index  int
	points []Point
}

type ClusterPoint struct {
	index int
	point Point
}

func (k Kmeans) euklideanDistance(a Point, b Point) float64 {
	sum := 0.0
	for i := 0; i < k.dimension; i++ {
		sum += math.Pow(a.numbers[i]-b.numbers[i], 2.0)
	}
	return math.Sqrt(sum)
}

func (k Kmeans) getClosestCentroid(centroids []Point, point Point) int {
	closest := -1
	distance := math.MaxFloat64
	for i := 0; i < len(centroids); i++ {
		newDistance := k.euklideanDistance(point, centroids[i])
		if newDistance < distance {
			distance = newDistance
			closest = i
		}
	}
	return closest
}

func (k Kmeans) countNewCentroidForCluster(cluster []Point) Point {
	newCentroidPoint := make([]float64, k.dimension)
	newCentroid := Point{
		newCentroidPoint,
		k.dimension,
	}
	for _, point := range cluster {
		for i := 0; i < k.dimension; i++ {
			newCentroid.numbers[i] += point.numbers[i]
		}
	}

	for i := 0; i < k.dimension; i++ {
		newCentroid.numbers[i] /= float64(len(cluster))
	}
	return newCentroid
}

func (k Kmeans) centroidWorker(taskChannel chan Cluster, centroidResChannel chan ClusterPoint) {
	for {
		value, more := <-taskChannel
		if more {
			//println("Counting")
			centroidResChannel <- ClusterPoint{value.index, k.countNewCentroidForCluster(value.points)}
		} else {
			//println("Closing")
			//close(centroidResChannel)
			return
		}
	}
}

func (k Kmeans) centroidResultWorker(centroidResChannel chan ClusterPoint, endChannel chan bool, centroids []Point) {
	proccessed := 0
	for {
		value, more := <-centroidResChannel
		if more {
			//println("Assigning")
			centroids[value.index] = value.point
		} else {
			//println("Ending")
			break
		}
		proccessed++
		if proccessed == k.k {
			endChannel <- true
			break
		}
	}
}

func (k Kmeans) createNewCentroids(clusters [][]Point) []Point {
	centroids := make([]Point, len(clusters))
	taskChannel := make(chan Cluster)
	centroidResChannel := make(chan ClusterPoint)
	endChannel := make(chan bool)
	launchedThreads := 0
	for i := 0; i < k.k; i++ {
		if i >= k.numOfThreads {
			break
		}
		go k.centroidWorker(taskChannel, centroidResChannel)
		launchedThreads++
	}

	go k.centroidResultWorker(centroidResChannel, endChannel, centroids)

	for index, cluster := range clusters {
		//println(cluster)
		clus := Cluster{index, cluster}
		taskChannel <- clus
	}
	close(taskChannel)
	<-endChannel
	close(centroidResChannel)
	close(endChannel)
	return centroids
}

func (k Kmeans) centroidSearchWorkerParallel(points []Point, resChannel chan []ClusterPoint, centroids []Point, start int, end int) {
	clusterPoints := make([]ClusterPoint, 0)
	for i := start; i < end; i++ {
		clusterPoints = append(clusterPoints, ClusterPoint{k.getClosestCentroid(centroids, points[i]), points[i]})
	}
	resChannel <- clusterPoints
}

func (k Kmeans) clusterPointAsignWorker(resChannel chan []ClusterPoint, endChannel chan bool, clusters [][]Point) {
	processed := 0
	for {
		value, more := <-resChannel
		if more {
			//println("Assigning to cluster")
			for _, clusterPoint := range value {
				clusters[clusterPoint.index] = append(clusters[clusterPoint.index], clusterPoint.point)
			}
			processed++
			if processed == k.numOfThreads {
				endChannel <- true
				break
			}
			//println(value.point.numbers)

		} else {
			//println("Ending")
			endChannel <- true
			break
		}
	}

}

func (k Kmeans) initClustersParallel(points []Point, centroids []Point) [][]Point {
	clusters := make([][]Point, len(centroids))
	resChannel := make(chan []ClusterPoint)
	endChannel := make(chan bool)
	for i := 0; i < k.k; i++ {
		clusters[i] = make([]Point, 0)
	}
	chunkSize := int(len(points) / k.numOfThreads)
	end := 0
	start := 0
	for i := 0; i < k.numOfThreads; i++ {
		start = chunkSize * i
		if i == k.numOfThreads-1 {
			end = len(points)
		} else {
			end = chunkSize * (i + 1)
		}
		go k.centroidSearchWorkerParallel(points, resChannel, centroids, start, end)
	}
	go k.clusterPointAsignWorker(resChannel, endChannel, clusters)

	<-endChannel
	close(resChannel)
	close(endChannel)
	return clusters
}

func (k Kmeans) checkCentroidChange(centroids []Point, newCentroids []Point) bool {
	centroidChange := 0.0
	for index, point := range centroids {
		centroidChange += k.euklideanDistance(point, newCentroids[index])
	}
	return centroidChange > k.minChange
}

func numInSlice(num int, slice []int) bool {
	for _, number := range slice {
		if num == number {
			return true
		}
	}
	return false
}

func (k Kmeans) initializeClustersSerial(points []Point, centroids []Point) [][]Point {
	clusters := make([][]Point, k.k)
	for _, point := range points {
		index := k.getClosestCentroid(centroids, point)
		clusters[index] = append(clusters[index], point)
	}
	return clusters
}

func (k Kmeans) createNewCentroidsSerial(clusters [][]Point) []Point {
	centroids := make([]Point, 0)
	for _, cluster := range clusters {
		centroids = append(centroids, k.countNewCentroidForCluster(cluster))
	}
	return centroids
}

func (k Kmeans) doKmeansParallel() [][]Point {
	centroids := make([]Point, k.k)
	occured := make([]int, 0)
	numOfPoints := len(k.points)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < k.k; i++ {
		random := rand.Intn(numOfPoints)
		for {
			if !numInSlice(random, occured) {
				break
			}
			random = rand.Intn(numOfPoints)
		}
		occured = append(occured, random)
		centroids[i] = k.points[random]
	}
	clusters := k.initClustersParallel(k.points, centroids)
	newCentroids := k.createNewCentroids(clusters)
	for {
		if !k.checkCentroidChange(centroids, newCentroids) {
			break
		}
		centroids = newCentroids
		clusters = k.initClustersParallel(k.points, centroids)
		newCentroids = k.createNewCentroids(clusters)
	}
	return clusters
}

func (k Kmeans) doKmeansSerial() [][]Point {
	centroids := make([]Point, k.k)
	occured := make([]int, 0)
	numOfPoints := len(k.points)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < k.k; i++ {
		random := rand.Intn(numOfPoints)
		for {
			if !numInSlice(random, occured) {
				break
			}
			random = rand.Intn(numOfPoints)
		}
		occured = append(occured, random)
		centroids[i] = k.points[random]
	}
	clusters := k.initializeClustersSerial(k.points, centroids)
	newCentroids := k.createNewCentroidsSerial(clusters)
	for {
		if !k.checkCentroidChange(centroids, newCentroids) {
			break
		}
		centroids = newCentroids
		clusters = k.initializeClustersSerial(k.points, centroids)
		newCentroids = k.createNewCentroidsSerial(clusters)
	}
	return clusters
}
