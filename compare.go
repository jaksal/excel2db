package main

import (
	"log"
	"reflect"
	"strconv"
)

func getkey(src []interface{}, keys []int) string {
	if len(src) < len(keys) {
		return ""
	}

	var result string
	for _, k := range keys {
		switch src[k].(type) {
		case int, int32, int64:
			result += strconv.Itoa(src[k].(int))
		case float32, float64:
			result += strconv.FormatFloat(src[k].(float64), 'f', -1, 64)
		case string:
			result += src[k].(string)
		}
	}
	return result
}

func compareData(src, dst *SheetData) bool {
	// check header.
	if len(src.header) != len(dst.header) {
		log.Println("mismatch header len", src.header, dst.header)
		return false
	}

	var keys []int
	for i := 0; i < len(src.header); i++ {
		if src.header[i].Column != dst.header[i].Column {
			log.Println("mismatch header", src.header, dst.header)
			return false
		}

		if src.header[i].isKey {
			keys = append(keys, i)
		}
	}

	// rebuild data
	srcData := make(map[string][]interface{})
	for _, row := range src.data {
		srcData[getkey(row, keys)] = row
	}
	dstData := make(map[string][]interface{})
	for _, row := range dst.data {
		dstData[getkey(row, keys)] = row
	}

	bEqual := true
	// compare data
	for skey, sval := range srcData {
		if dval, exist := dstData[skey]; exist {
			// compare row
			for i := 0; i < len(sval); i++ {
				if !reflect.DeepEqual(sval[i], dval[i]) {
					log.Printf("diff key=%s row:%s %v<=>%v\n", skey, src.header[i].Column, sval[i], dval[i])
					bEqual = false
				}
			}
			delete(dstData, skey)
			delete(srcData, skey)
		}
	}
	for skey, sval := range srcData {
		log.Printf("del key=%s row:%+v\n", skey, sval)
		bEqual = false
	}
	for skey, dval := range dstData {
		log.Printf("new key=%s row:%+v\n", skey, dval)
		bEqual = false
	}

	return bEqual
}
