package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/kardianos/osext"
	"github.com/tealeg/xlsx"
)

var debug bool

func main() {
	var reload bool
	var sheet string
	var checkDB bool
	var serverTag string
	var xlsFiles []string
	var compare string
	var server *ServerConf

	// parse config
	{
		flag.BoolVar(&debug, "debug", false, "debug mode")
		flag.BoolVar(&reload, "reload", false, "reload data")
		flag.StringVar(&serverTag, "server", "dev", "target server")
		flag.StringVar(&sheet, "sheet", "all", "select sheet")
		flag.BoolVar(&checkDB, "check_db", true, "check validate data")
		flag.StringVar(&compare, "compare", "", "compare data xls <==>db or server")

		flag.Parse()

		xlsFiles = flag.Args()
		if len(xlsFiles) == 0 {
			log.Fatalln("input file parameter error!")
		}
		log.Println("debug  is ", debug)
	}

	// read server config..
	{
		exePath, _ := osext.ExecutableFolder()
		var err error
		server, err = ReadServerConf(exePath+"/conf.json", serverTag)
		if err != nil {
			log.Fatalln("read config fail! conf.json", err)
		}
		log.Printf("target server db=%s redis=%+v\n", server.Db, server.Redis)
	}

	// proc xls files..
	for _, path := range xlsFiles {

		// load sheetconf.
		dir, file := filepath.Split(path)

		// load config.
		name := dir + "conf/" + strings.TrimSuffix(file, filepath.Ext(file)) + ".json"
		sheetConfs, err := ReadSheetConf(name, strings.Split(sheet, ","))
		if err != nil {
			log.Fatalln(err)
		}

		if err := proc(path, sheetConfs, server, compare, reload, checkDB); err != nil {
			log.Fatalln("err", err)
		}
	}
	//
}

func proc(path string, sheetConfs SheetConfs, server *ServerConf, compare string, reload, checkDB bool) error {

	log.Println("=========================load xls file=========================\n", path)
	// load xlsx.
	xlFile, err := xlsx.OpenFile(path)
	if err != nil {
		return fmt.Errorf("xlsx read fail! path=%s err=%s", path, err)
	}

	var reloadStr []string

	for key, conf := range sheetConfs {
		log.Println("=========================parse sheet=========================\n", key)
		if debug {
			spew.Dump(conf)
		}

		sheet, ok := xlFile.Sheet[key]
		if !ok {
			log.Println("not found sheet! ", key)
			break
		}

		data, err := loadXlsSheet(sheet, conf)
		if err != nil {
			log.Fatalln("loadXlsSheet fail!", err)
		}

		if compare == "" {
			// loadDBData(conf)
			log.Println("=========================insert data=========================")
			if err := dbInsertAll(data, conf.Table, server.Db, checkDB); err != nil {
				log.Fatalln(err)
			}

			if reload && conf.Reload != "" {
				reloadStr = append(reloadStr, conf.Reload)
			}

		} else if compare == "db" {
			log.Println("=========================get data from db!!!=========================")

			if result, err := loadDBData(conf, server.Db[0]); err != nil {
				log.Fatal(err)
			} else {
				log.Println("=========================compare!!!=========================")
				// compare ..
				log.Println("compare result ", compareData(data, result))
			}
		} else if compare == "server" {

			if server.Server != "" && conf.CheckURL != "" {
				log.Println("=========================get data from web!!!=========================")

				if result, err := loadWebData(conf, server.Server, conf.CheckURL); err != nil {
					log.Fatal(err)
				} else {
					log.Println("=========================compare!!!=========================")
					// compare ..
					log.Println("compare result ", compareData(data, result))
				}
			}

		}

		log.Println("=========================finish!!!=========================")
	}

	if reload && len(reloadStr) != 0 {
		sendReload(removeDuplicate(reloadStr), server.Redis)
	}
	return nil
}
