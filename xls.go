package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/metakeule/fmtdate"
	"github.com/tealeg/xlsx"
)

// SheetData xls sheet data
type SheetData struct {
	header []*Col
	data   [][]interface{}
}

func loadXlsSheet(sheet *xlsx.Sheet, conf *SheetConf) (*SheetData, error) {

	headIdx := 0
	if conf.HeadLine == 0 {
		headIdx = 1
	} else {
		headIdx = conf.HeadLine - 1
	}

	result := &SheetData{
		header: make([]*Col, len(conf.Cols)),
	}

	// get header
	idx := 0
	for cellIdx, cell := range sheet.Rows[headIdx].Cells {

		colName := strings.TrimSpace(cell.String())
		if col, exist := conf.Cols[colName]; exist {
			col.cellIdx = cellIdx

			result.header[idx] = col

			if debug {
				log.Println("read column", idx, colName, col, cellIdx)
			}
			idx++
		} else {
			if debug {
				log.Println("ignore column", idx, colName)
			}
		}
		if idx == len(conf.Cols) {
			break
		}
	}

	// check.. header.
	for name, col := range conf.Cols {
		bFind := false
		for _, h := range result.header {
			if h != nil && col.Column == h.Column {
				bFind = true
				break
			}
		}

		if bFind == false {
			return nil, fmt.Errorf("not found column in xls sheet! (but exist in config file) name=%s", name)
		}
	}

	//
	for rowIdx, row := range sheet.Rows[headIdx+1:] {
		rowData := make([]interface{}, len(result.header))
		cellLen := len(row.Cells)

		if cellLen == 0 {
			break
		}
		if row.Cells[0].Hidden ||
			(row.Cells[0].GetStyle().Fill.FgColor != "" && row.Cells[0].GetStyle().Fill.FgColor != "FFFFFFFF") ||
			(row.Cells[0].GetStyle().Fill.BgColor != "" && row.Cells[0].GetStyle().Fill.FgColor != "FFFFFFFF") {
			if debug {
				log.Println("ignore hidden or color row", rowIdx, row.Cells[0].Hidden, row.Cells[0].GetStyle().Fill.FgColor, row.Cells[0].GetStyle().Fill.BgColor)
			}
			continue
		}

		bCheck := false
		for idx, h := range result.header {
			rowData[idx] = h.DefaultData()

			if h.cellIdx < cellLen {
				switch h.Format {
				case "int":
					if tempInt, err := row.Cells[h.cellIdx].Int(); err == nil {
						rowData[idx] = tempInt
					}
				case "float":
					if tempFloat, err := row.Cells[h.cellIdx].Float(); err == nil {
						rowData[idx] = tempFloat
					}
				case "string":
					rowData[idx] = row.Cells[h.cellIdx].String()
				case "datetime":
					if tempStr := row.Cells[h.cellIdx].String(); tempStr != "" {
						if _, err := fmtdate.Parse("YYYY-MM-DD hh:mm:ss", tempStr); err != nil {
							log.Fatalln("invalid date string", tempStr, err)
						}
						rowData[idx] = tempStr
					}
				}
			}

			if h.isKey && rowData[idx] == h.DefaultData() {
				bCheck = true
			}
		}

		if bCheck {
			if debug {
				log.Println("ignore check key!", rowData)
			}
			continue
		}

		if debug {
			log.Println("read row", rowData)
		}
		result.data = append(result.data, rowData)
	}

	return result, nil
}
