package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/go-sql-driver/mysql"
)

// Col xls column
type Col struct {
	Column string `json:"column"` // db column name
	Format string `json:"format"` // data format : int,float,string,datetime

	cellIdx int
	isKey   bool
}

func (c *Col) String() string {
	return fmt.Sprintf("column=%s format=%s cell_idx=%d", c.Column, c.Format, c.cellIdx)
}

// DefaultData get col default data
func (c *Col) DefaultData() interface{} {
	switch c.Format {
	case "int":
		return 0
	case "float":
		return 0.0
	case "string":
		return ""
	case "datetime":
		return mysql.NullTime{}
	}
	return nil
}

// SheetConf xls sheet config
type SheetConf struct {
	CheckURL string          `json:"check_url"`
	Reload   string          `json:"reload"`
	Table    string          `json:"table"`
	Keys     []string        `json:"keys"`
	HeadLine int             `json:"head_line"`
	Cols     map[string]*Col `json:"cols"` // key is xls column name
}

// SheetConfs sheetconfig list
type SheetConfs map[string]*SheetConf // key is sheet name
/*
func (c *SheetConf) isKey(name string) bool {
	for _, key := range c.Keys {
		if name == key {
			return true
		}
	}
	return false
}
*/

// ReadSheetConf read xls sheet conf
func ReadSheetConf(path string, sheets []string) (SheetConfs, error) {

	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("invalid sheet config path=%s err=%s", path, err)
	}

	var src SheetConfs
	if err := json.Unmarshal(dat, &src); err != nil {
		return nil, fmt.Errorf("config parse error path=%s err=%s", path, err)
	}

	// check and mark key
	for n, s := range src {
		for _, k := range s.Keys {
			exist := false
			for _, c := range s.Cols {
				if c.Column == k {
					exist = true
					c.isKey = true
					break
				}
			}
			if !exist {
				return nil, fmt.Errorf("not found key from col list sheet=%s key=%s", n, k)
			}
		}
	}

	if len(sheets) == 0 || sheets[0] == "all" {
		return src, nil
	}

	result := make(SheetConfs)
	for _, s := range sheets {
		if dst, exist := src[s]; exist {
			result[s] = dst
		}
	}
	return result, nil
}
