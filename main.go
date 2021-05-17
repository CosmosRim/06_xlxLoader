package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"sync"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/BurntSushi/toml"
	_ "github.com/mattn/go-oci8"
)

const cfgPath string = "./config/"
const cfgFile string = "config.toml"
const xlxPath string = "./xlsxFiles/"
const logPath string = "./log/"

//define structs to get info from config.toml
type cfgInfo struct {
	DBType        string
	MaxGoroutines int64
	DB            DBInfo    `toml:"DBInfo"`
	TbList        TableList `toml:"tableList"`
}
type DBInfo struct {
	User string `toml:"DBUser"`
	Pwd  string `toml:"DBPwd"`
	IP   string `toml:"DBIP"`
	Port int    `toml:"DBPort"`
	Sid  string `toml:"DBSid"`
}
type TableList struct {
	TableList [][]string `toml:"list"`
}

func getTomlInfo(file string, st *cfgInfo) {
	if _, err := toml.DecodeFile(cfgPath+file, st); err != nil {
		panic(err)
	}
}

// func getTableList(file string, st *cfgInfo) {
// 	if _, err := toml.DecodeFile(cfgPath+file, st); err != nil {
// 		panic(err)
// 	}
// }

//get xlsx info and generate check/drop/create/insert sql
func readExcel(cfg cfgInfo, name int) (string, string, string, string, string, int /*, int, int, int */) {
	//open config.toml
	// var cfg cfgInfo
	// getTomlInfo(cfgFile, &cfg)
	tbName := cfg.TbList.TableList[name][0]
	xlxName := cfg.TbList.TableList[name][1]
	xlxSheet := cfg.TbList.TableList[name][2]
	f, err := excelize.OpenFile(xlxPath + xlxName)
	if err != nil {
		log.Println(err)
	}
	//use info from toml to generate drop table sql
	ckSql := fmt.Sprintf("select /*+parallel(16)*/ count(*) from USER_TABLES where TABLE_NAME=upper('%s')", tbName)
	dtSql := fmt.Sprintf("drop table %s", tbName)

	//get cell style
	// style1 := f.GetCellFormula(cfg.TbList.TableList[name][2], "A2")
	// style2 := f.GetCellStyle(cfg.TbList.TableList[name][2], "B2")
	// style3 := f.GetCellStyle(cfg.TbList.TableList[name][2], "C2")

	//open current excel
	rows := f.GetRows(xlxSheet)
	if err != nil {
		log.Println(err)
	}

	//use first row from excel to generate create table sql
	definRow := rows[0]
	var ctSql string
	for col, colCell := range definRow {
		switch {
		case col == 0:
			ctSql += fmt.Sprintf("create table %s (%s varchar2(255), ", tbName, colCell)
		case col > 0 && col < len(definRow)-1:
			ctSql += fmt.Sprintf("%s varchar2(255), ", colCell)
		case col == len(definRow)-1:
			ctSql += fmt.Sprintf("%s varchar2(255))", colCell)
		default:
			log.Printf("When generate ctSql of %s, using %s_%s, meet unexpected length \"%d\" of first row.\n", tbName, xlxName, xlxSheet, col)
		}
	}

	count := 0
	istSql := fmt.Sprintf("insert all \n")
	for i := 1; i < len(rows); i++ {
		for col, colCell := range rows[i] {
			switch {
			case col == 0:
				istSql += fmt.Sprintf("into %s values ('%s', ", tbName, colCell)
			case col < len(rows[i])-1:
				istSql += fmt.Sprintf("'%s', ", colCell)
			case col == len(rows[i])-1:
				istSql += fmt.Sprintf("'%s')\n", colCell)
			}
		}
		count++
	}
	istSql += fmt.Sprintf("select 1 from dual")

	return tbName, ckSql, dtSql, ctSql, istSql, count /* , style1, style2, style3 */
}

func main() {
	//remove old log folder and create new one
	if _, err := os.Stat(logPath); err == nil {
		log.Println("Remove exists old folder : " + logPath)
		os.RemoveAll(logPath)
	}

	if err := os.Mkdir(logPath, os.ModePerm); err != nil {
		log.Println("Create log folder: " + logPath + " failed.")
	} else {
		log.Println("Create log folder: " + logPath + " succeed.")
	}

	//get dsn info
	var cfg cfgInfo
	getTomlInfo(cfgFile, &cfg)
	dsn := fmt.Sprintf("%s/%s@%s:%d/%s", cfg.DB.User, cfg.DB.Pwd, cfg.DB.IP, cfg.DB.Port, cfg.DB.Sid)
	log.Println("warming engine...")
	log.Println("dsn is: " + dsn)

	//create connection with oDB
	db, err := sql.Open("oci8", dsn)
	if err != nil {
		panic(err)
	}
	if db == nil {
		log.Println("db is nil")
	}
	defer db.Close()

	//start monitor of goroutines
	go func() {
		log.Println("pprof start")
		fmt.Println(http.ListenAndServe(":12610", nil))
	}()

	//counter of success excels
	var countXlx int64
	//make a chan/tunnel to limit max multi goroutines
	wg := &sync.WaitGroup{}
	limiter := make(chan bool, cfg.MaxGoroutines)
	//deal all excels in toml list
	for tbNumber := 0; tbNumber < len(cfg.TbList.TableList); tbNumber++ {
		wg.Add(1)
		limiter <- true

		//use new variable to store number, or it will mixup in multiple goroutines
		tbLopNum := tbNumber //use

		go func() {
			//sometimes run quickly again will leading oci connectiong close before goroutines finish, not clear reason yet.
			defer wg.Done()
			defer func() {
				<-limiter
			}()

			//generate all sqls from excel
			tbName, ckSql, dtSql, ctSql, istSql, countLines /* , st1, st2, st3 */ := readExcel(cfg, tbLopNum)

			log.Println(tbName + " dealed")
			// log.Printf("A: %d, B: %d, C: %d", st1, st2, st3)

			//create log files and start a log object
			logFileName := logPath + tbName + ".log"
			if _, err := os.Stat(logFileName); err == nil {
				//log.Println("Remove exists old log file: " + logFileName)
				os.Remove(logFileName)
			}

			logFile, err := os.Create(logFileName)
			if err != nil {
				log.Fatalf("Create log file with error: %s", err)
			}
			defer logFile.Close()

			logOb := log.New(logFile, "", log.LstdFlags)

			//check current table exists in table or not
			exists, err := db.Query(ckSql)
			if err != nil {
				logOb.Println(err)
				// runtime.Goexit()
			}
			var tmpExist uint8
			for exists.Next() {
				exists.Scan(&tmpExist)
			}
			exists.Close()

			//if table exist, drop first. Then recreate table and insert all values.
			if tmpExist == 1 {
				drop, err := db.Query(dtSql)
				if err != nil {
					logOb.Println(err)
					// runtime.Goexit()
				}
				drop.Close()
			}

			create, err := db.Query(ctSql)
			if err != nil {
				logOb.Println(err)
				runtime.Goexit()
			}
			create.Close()
			logOb.Printf("table %s recreated", tbName)

			insert, err := db.Query(istSql)
			if err != nil {
				logOb.Println(err)
				runtime.Goexit()
			}
			insert.Close()
			logOb.Printf("table %s inserted", tbName)

			commit, err := db.Query("commit")
			if err != nil {
				log.Println(err)
			}
			commit.Close()

			logOb.Printf("%s finished, and %d records have been inserted.", tbName, countLines)

			// wg.Done()
			// func() {
			// 	<-limiter
			// }()
			runtime.Goexit()
		}()

		countXlx++
	}

	wg.Wait()

	log.Printf("All finished,  %d tables have been loaded.", countXlx)
}
