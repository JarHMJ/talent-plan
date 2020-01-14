package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.

var nCPU = runtime.NumCPU()
var wg sync.WaitGroup

type item struct {
	value    int64
	row, col int
}
type itemHeap []item

func (hp *itemHeap) Push(x interface{}) {
	*hp = append(*hp, x.(item))
}

func (hp *itemHeap) Pop() interface{} {
	n := len(*hp)
	x := (*hp)[n-1]
	*hp = (*hp)[:n-1]
	return x
}

func (hp itemHeap) Len() int {
	return len(hp)
}

func (hp itemHeap) Less(i, j int) bool {
	return hp[i].value < hp[j].value
}

func (hp itemHeap) Swap(i, j int) {
	hp[i], hp[j] = hp[j], hp[i]
}

func MergeSort(src []int64) {
	//mergeUp2Down(src, 0, len(src)-1)
	gap := int(math.Ceil(float64(len(src)) / float64(nCPU)))
	for i := 0; i < nCPU; i++ {
		var tmpSrc []int64
		if i+gap > len(src)-1 {
			tmpSrc = src[i*gap:]
		} else {
			tmpSrc = src[i*gap : (i+1)*gap]
		}
		wg.Add(1)
		go innerSort(tmpSrc)
	}
	wg.Wait()
	merge(src)
}

func innerSort(src []int64) {
	defer wg.Done()
	sort.Slice(src, func(i, j int) bool { return src[i] < src[j] })
}

func merge(src []int64) {
	array := make([][]int64, nCPU)
	gap := int(math.Ceil(float64(len(src)) / float64(nCPU)))
	hp := make(itemHeap, nCPU)
	for i := 0; i < nCPU; i++ {
		var tmpSrc []int64
		if i+gap > len(src)-1 {
			tmpSrc = src[i*gap:]
		} else {
			tmpSrc = src[i*gap : (i+1)*gap]
		}
		array[i] = tmpSrc
		heap.Push(&hp, item{tmpSrc[0], i, 0})
	}
	for i := 0; i < len(src); i++ {
		min := (heap.Pop(&hp)).(item)
		src[i] = min.value
		if min.col < len(array[min.row])-1 {
			heap.Push(&hp, item{array[min.row][min.col+1], min.row, min.col + 1})
		}
	}
}

func main() {
	num := 20
	src := make([]int64, num)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < num; i++ {
		src[i] = rand.Int63n(100)
	}
	fmt.Println(src)
	fmt.Printf("%T\n", src)
	MergeSort(src)
	fmt.Println(src)
}
