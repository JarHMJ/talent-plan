package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.

//var nCPU = runtime.NumCPU()
var nCPU = 4
var wg sync.WaitGroup

type item struct {
	value    int64
	row, col int
}
type itemHeap []*item

func (hp *itemHeap) Push(x interface{}) {
	*hp = append(*hp, x.(*item))
}

func (hp *itemHeap) Pop() interface{} {
	n := len(*hp)
	x := (*hp)[n-1]
	*hp = (*hp)[0 : n-1]
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
	gap := int(math.Ceil(float64(len(src)) / float64(nCPU)))
	//l := len(src)
	//for i := 0; i < nCPU; i++ {
	//	wg.Add(1)
	//	tmpSrc := src[i*l/nCPU : (i+1)*l/nCPU]
	//	go func() {
	//		defer wg.Done()
	//		sort.Slice(tmpSrc, func(i, j int) bool { return tmpSrc[i] < tmpSrc[j] })
	//	}()
	//}
	for i := 0; i < len(src); i += gap {
		var tmpSrc []int64
		if i+gap > len(src)-1 {
			tmpSrc = src[i:]
		} else {
			tmpSrc = src[i : i+gap]
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
	tmp := make([]int64, len(src))
	copy(tmp, src)
	array := make([][]int64, nCPU)
	gap := int(math.Ceil(float64(len(src)) / float64(nCPU)))
	hp := make(itemHeap, 0)
	f := true
	//l := len(src)
	//for i := 0; i < nCPU; i++ {
	//	tmpSrc := tmp[i*l/nCPU : (i+1)*l/nCPU]
	//	array[i] = tmpSrc
	//	heap.Push(&hp, item{tmpSrc[0], i, 0})
	//}
	for i := 0; i < nCPU && f; i++ {
		var tmpSrc []int64
		if (i+1)*gap > len(src)-1 {
			tmpSrc = tmp[i*gap:]
			f = false
		} else {
			tmpSrc = tmp[i*gap : (i+1)*gap]
		}
		array[i] = tmpSrc
		//hp[i] = item{tmpSrc[0], i, 0}
		heap.Push(&hp, &item{tmpSrc[0], i, 0})
	}
	//fmt.Println(array)
	//fmt.Println(hp)
	for i := 0; i < len(src); i++ {
		min := (heap.Pop(&hp)).(*item)
		//fmt.Println(array)
		//fmt.Println(hp)
		src[i] = min.value
		if min.col < len(array[min.row])-1 {
			//heap.Push(&hp, item{array[min.row][min.col+1], min.row, min.col + 1})
			min.value = array[min.row][min.col+1]
			min.col +=1
			heap.Push(&hp, min)
		}
		//fmt.Println(hp)
	}
}

func main() {
	num := 5
	src := make([]int64, num)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < num; i++ {
		src[i] = rand.Int63n(100)
	}
	fmt.Println(src)
	fmt.Printf("%T\n", src)
	MergeSort(src)
	//l:=len(src)
	//for i := 0; i < nCPU; i++ {
	//	fmt.Println(src[i*l/nCPU:(i+1)*l/nCPU])
	//}
	fmt.Println(src)
}
