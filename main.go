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
const cfgFile string = "config.toml"
const xlxPath string = "./xlsxFiles/"

//define structs to get info from config.toml
type cfgInfo struct {
	DB     DBInfo    `toml:"DBInfo"`
	TbList TableList `toml:"tableList"`
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
func readExcel(cfg cfgInfo, name int) (string, string, string, string, string, int /* , int, int, int */) {
	//open config.toml
	// var cfg cfgInfo
	// getTomlInfo(cfgFile, &cfg)
	tbName := cfg.TbList.TableList[name][0]
	xlxName := cfg.TbList.TableList[name][1]
	xlxSheet := cfg.TbList.TableList[name][2]
	f, err := excelize.OpenFile(xlxPath + "/" + xlxName)
	if err != nil {
		log.Println(err)
	}
	//use info from toml to generate drop table sql
	ckSql := fmt.Sprintf("select /*+parallel(16)*/ count(*) from USER_TABLES where TABLE_NAME=upper('%s')", tbName)
	dtSql := fmt.Sprintf("drop table %s", tbName)

	//get cell style
	// style1 := f.GetCellStyle(cfg.TbList.TableList[name][2], "A2")
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

	for tbNumber := 0; tbNumber < len(cfg.TbList.TableList); tbNumber++ {
		tbName, ckSql, dtSql, ctSql, istSql, counts := readExcel(cfg, tbNumber)

		//check current table exists in table or not
		exists, err := db.Query(ckSql)
		if err != nil {
			log.Println(err)
		}
		var tmpExist uint8
		for exists.Next() {
			exists.Scan(&tmpExist)
		}
		defer exists.Close()

		if tmpExist == 1 {
			drop, err := db.Query(dtSql)
			if err != nil {
				log.Println(err)
			}
			defer drop.Close()
		}

		create, err := db.Query(ctSql)
		if err != nil {
			log.Println(err)
		}
		defer create.Close()
		log.Printf("table %s recreated", tbName)

		insert, err := db.Query(istSql)
		if err != nil {
			log.Println(err)
		}
		defer insert.Close()
		log.Printf("table %s inserted", tbName)

		// insertSql := fmt.Sprintf("insert all ")
		// for i := 2; i <= cols; i++ {
		// 	v := f.GetCellValue("Sheet1", fmt.Sprintf("A%d", i))
		// 	k := f.GetCellValue("Sheet1", fmt.Sprintf("B%d", i))
		// 	if k != "" {
		// 		insertSql += fmt.Sprintf("into mig_tmp_mappingIndustry values ('%s','%s',%d) ", k, v, i)
		// 		counts++
		// 	}
		// }

		// insertSql += fmt.Sprintf("select 1 from dual")
		// insert, err := db.Query(insertSql)
		// if err != nil {
		// 	log.Println(err)
		// }
		// defer insert.Close()
		commit, err := db.Query("commit")
		if err != nil {
			log.Println(err)
		}
		defer commit.Close()

		log.Printf("%s finished, and %d records have been inserted.", tbName, counts)
	}

}
