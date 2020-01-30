package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"strconv"
	"strings"
)

type UCHeap []*urlCount

func (uch UCHeap) Len() int {
	return len(uch)
}

func (uch UCHeap) Less(i, j int) bool {
	if uch[i].cnt == uch[j].cnt {
		// 假如相同数量，key的值大的排在前面
		return uch[i].url > uch[j].url
	}
	return uch[i].cnt < uch[j].cnt
}

func (uch UCHeap) Swap(i, j int) {
	uch[i], uch[j] = uch[j], uch[i]
}

func (uch *UCHeap) Push(x interface{}) {
	item := x.(*urlCount)
	*uch = append(*uch, item)
}

func (uch *UCHeap) Pop() interface{} {
	n := len(*uch)
	item := (*uch)[n-1]
	*uch = (*uch)[:n-1]
	return item
}

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	// round 1: do url count
	// 这个阶段用的map和reduce和example里的一样
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: 在map中先获取top10的数据，把排序的过程放到map中，然后reduce做一下最终的汇总
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvs = append(kvs, KeyValue{Key: l})
	}
	return kvs
}

// ExampleURLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(len(values)))
}

// ExampleURLTop10Map is the map function in the second round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	// 先获取每个map中的top10
	//cnts := make(map[string]int, len(lines))
	//for _, v := range lines {
	//	v := strings.TrimSpace(v)
	//	if len(v) == 0 {
	//		continue
	//	}
	//	tmp := strings.Split(v, " ")
	//	n, err := strconv.Atoi(tmp[1])
	//	if err != nil {
	//		panic(err)
	//	}
	//	cnts[tmp[0]] = n
	//}
	//us, cs := TopN(cnts, 10)
	// 这里用堆来获取top10
	heapCap := len(lines)
	if heapCap > 10 {
		heapCap = 10
	}
	ucHeap := make(UCHeap, heapCap)
	for i := 0; i < heapCap; i++ {
		ucHeap[i] = &urlCount{"", 0}
	}
	heap.Init(&ucHeap)
	for _, v := range lines {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		if ucHeap[0].cnt < n {
			ucHeap[0].url = tmp[0]
			ucHeap[0].cnt = n
			heap.Fix(&ucHeap, 0)
		} else if ucHeap[0].cnt == n && ucHeap[0].url > tmp[0] {
			ucHeap[0].url = tmp[0]
			heap.Fix(&ucHeap, 0)
		}
	}

	//kvs := make([]KeyValue, 0, len(us))
	//for i, l := range us {
	//	kvs = append(kvs, KeyValue{"", fmt.Sprintf("%s %s\n", l, strconv.Itoa(cs[i]))})
	//}
	kvs := make([]KeyValue, 0, heapCap)
	for i := heapCap - 1; i >= 0; i-- {

		item := heap.Pop(&ucHeap).(*urlCount)
		if item.cnt == 0 {
			continue
		}
		kvs = append(kvs, KeyValue{"", fmt.Sprintf("%s %d\n", item.url, item.cnt)})
	}
	//fmt.Println(kvs)
	reverse(kvs)
	return kvs
}

func reverse(kvs []KeyValue) {
	l := len(kvs)
	for i := 0; i < l/2; i++ {
		kvs[i], kvs[l-i-1] = kvs[l-i-1], kvs[i]
	}
}

// ExampleURLTop10Reduce is the reduce function in the second round
func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		//fmt.Printf("tmp:{%v}", v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")

		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
