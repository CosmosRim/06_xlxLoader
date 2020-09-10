package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/BurntSushi/toml"
	_ "github.com/mattn/go-oci8"
)

const cfgPath string = "./config/"

type cfgInfo struct {
	Db           oDbInfo `toml:"oDbInfo"`
}
type oDbInfo struct {
	User string `toml:"oDbUser"`
	Pwd  string `toml:"oDbPwd"`
	IP   string `toml:"oDbIp"`
	Port int    `toml:"oDbPort"`
	Sid  string `toml:"oDbSid"`
}

func getODbInfo(fl string, st *cfgInfo) {
	if _, err := toml.DecodeFile(cfgPath+fl, st); err != nil {
		panic(err)
	}
}

func main() {
	//get dsn info
	var cfg cfgInfo
	getODbInfo("config.toml", &cfg)
	dsn := fmt.Sprintf("%s/%s@%s:%d/%s", cfg.Db.User, cfg.Db.Pwd, cfg.Db.IP, cfg.Db.Port, cfg.Db.Sid)
	log.Println("warming engine...")
	log.Println("dsn is: " + dsn)

	//open xlsx
	f, err := excelize.OpenFile("xlsxFiles/MAPPING INDUSTRY_BETWEEN_ZSMART_BSCS.xlsx")
	if err != nil {
		log.Println(err)
		return
	}

	//create connection with oDB
	db, err := sql.Open("oci8", dsn)
	if err != nil {
		panic(err)
	}
	if db == nil {
		log.Println("db is nil")
	}
	defer db.Close()

	exists, err := db.Query("select /*+parallel(16)*/ count(*) from USER_TABLES where TABLE_NAME=upper('mig_tmp_mappingIndustry')")
	if err != nil {
		log.Println(err)
	}
	var tmpExist uint8
	for exists.Next() { exists.Scan(&tmpExist) }
	defer exists.Close()

	if tmpExist == 1 {
		drop, err := db.Query("drop table mig_tmp_mappingIndustry")
		if err != nil {
			log.Println(err)
		}
		defer drop.Close()
	}

	create, err := db.Query("create table mig_tmp_mappingIndustry (bscs varchar2(255), cvbs varchar2(255), line number(5))")
	if err != nil {
		log.Println(err)
	}
	defer create.Close()
	log.Println("table mig_tmp_mappingIndustry recreated")

	//get all info from xlsx.sheet1, if B2 is empty then ignore
	rows := f.GetRows("Sheet1")
	cols := len(rows)
	counts := 0
	insertSql := fmt.Sprintf("insert all ")
	for i := 2; i <= cols; i++{
		v := f.GetCellValue("Sheet1", fmt.Sprintf("A%d", i))
		k := f.GetCellValue("Sheet1", fmt.Sprintf("B%d", i))
		if k != "" {
			insertSql += fmt.Sprintf("into mig_tmp_mappingIndustry values ('%s','%s',%d) ", k, v, i)
			counts++
		}
	}

	insertSql += fmt.Sprintf("select 1 from dual")
	insert, err := db.Query(insertSql)
	if err != nil {log.Println(err)}
	defer insert.Close()
	commit, err := db.Query("commit")
	if err != nil {log.Println(err)}
	defer commit.Close()

	log.Println("all finished, and",counts,"records of xlsx exists in bscs have been inserted.")
}
