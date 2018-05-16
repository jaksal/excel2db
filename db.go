package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func loadDBData(conf *SheetConf, server string) (*SheetData, error) {
	db, err := sql.Open("mysql", server)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	result := &SheetData{}

	// get header
	for _, col := range conf.Cols {
		result.header = append(result.header, col)
	}
	sort.Slice(result.header, func(i, j int) bool {
		return result.header[i].cellIdx < result.header[j].cellIdx
	})

	var colList []string
	for _, h := range result.header {
		colList = append(colList, h.Column)
	}

	query := "SELECT " + strings.Join(colList, ",") + " FROM " + conf.Table + " ORDER BY " + strings.Join(conf.Keys, ",")
	log.Println("[DB] ", query)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		rowData := make([]interface{}, len(result.header))
		scanArgs := make([]interface{}, len(result.header))
		for i := range rowData {
			scanArgs[i] = &rowData[i]
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		for idx, h := range result.header {
			if temp, ok := rowData[idx].([]byte); ok {
				switch h.Format {
				case "int":
					rowData[idx], _ = strconv.Atoi(string(temp))
				case "float":
					rowData[idx], _ = strconv.ParseFloat(string(temp), 64)
				case "string", "datetime":
					rowData[idx] = string(temp)
				}
			} else {
				rowData[idx] = h.DefaultData()
			}
		}
		if debug {
			log.Println("read row ", rowData)
		}

		result.data = append(result.data, rowData)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	return result, nil
}

func dbInsertAll(sheetData *SheetData, tableName string, server []string, checkDB bool) error {
	for _, c := range server {
		db, err := sql.Open("mysql", c)
		if err != nil {
			return fmt.Errorf("db open error addr=%s err=%s", c, err)
		}
		defer db.Close()

		isOK := false
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("tx begin error err=%s", err)
		}
		defer func() {
			if !isOK {
				tx.Rollback()
			}
		}()

		_, err = tx.Exec("DELETE FROM " + tableName)
		if err != nil {
			return fmt.Errorf("table clear error table=%s error=%s", tableName, err)
		}

		// generate insert query
		var colList []string
		var valList []string
		var paramList []int
		var updateColList []string

		for idx, h := range sheetData.header {
			colList = append(colList, h.Column)
			valList = append(valList, "?")
			paramList = append(paramList, idx)
		}
		for idx, h := range sheetData.header {
			if !h.isKey {
				updateColList = append(updateColList, h.Column+"=?")
				paramList = append(paramList, idx)
			}
		}

		query := "INSERT INTO " + tableName + "(" + strings.Join(colList, ",") + ") "
		query += "VALUES (" + strings.Join(valList, ",") + ") "
		query += "ON DUPLICATE KEY UPDATE "
		query += strings.Join(updateColList, ",")

		if debug {
			log.Println("SQL : ", query)
		}

		stmt, err := tx.Prepare(query)
		if err != nil {
			return fmt.Errorf("tx prepare error query=%s err=%s", query, err)
		}

		params := make([]interface{}, len(paramList))
		for _, row := range sheetData.data {
			for i, idx := range paramList {
				params[i] = row[idx]
				if tt, ok := row[idx].(string); ok && strings.Contains(tt, "\n") {
					fmt.Println(tt, "==>", hex.EncodeToString([]byte(tt)))
				}
			}

			if debug {
				log.Println(params)
			}

			_, err := stmt.Exec(params...)
			if err != nil {
				return fmt.Errorf("tx excute error query=%s param=%+v err=%s", query, params, err)
			}
		}

		if checkDB {
			log.Println("check.. validate base data...")
			var output sql.NullString
			if err := tx.QueryRow("SELECT fn_check_base_data() as output").Scan(&output); err != nil {
				return fmt.Errorf("db base data check error err=%s", err)
			}
			if output.Valid {
				return fmt.Errorf("db base data check error err=%s", output.String)
			}
		}

		isOK = true
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("tx commit err=%s", err)
		}
		log.Println("commit...OK")
	}
	return nil
}
