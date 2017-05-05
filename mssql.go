package data

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/satori/go.uuid"
)

type MsSQL struct {
	appDb  *sql.DB
	logsDb *sql.DB
}

type DbConns struct {
	AppConn  string
	LogsConn string
}

type SpCallLog struct {
	spCallLogId    uuid.UUID
	apiAccessLogId uuid.UUID
	dbName         string
	spName         string
	params         string
	duration       float64
	durationEx     float64
	errorCode      int
	errorMessage   string
}

func New(c DbConns) (*MsSQL, error) {
	appDb, err := sql.Open("mssql", c.AppConn)
	if err != nil {
		fmt.Println("Cannot connect to appDb: ", err.Error())
	}

	logsDb, err := sql.Open("mssql", c.LogsConn)
	if err != nil {
		fmt.Println("Cannot connect to logsDb: ", err.Error())
	}

	return &MsSQL{appDb, logsDb}, err
}

func (db *MsSQL) Close() {
	if db.appDb != nil {
		db.appDb.Close()
	}
	if db.logsDb != nil {
		db.logsDb.Close()
	}
}

func (db *MsSQL) CallSp(spName string, params string) (string, SpCallLog) {
	fmt.Printf("EXEC %s '%s'\n", spName, params)
	tm0 := time.Now()
	rows, err := db.appDb.Query("EXEC CallSp ?1, ?2", spName, params)
	durationEx := time.Since(tm0).Seconds() * 1000 //in ms
	if err != nil {
		fmt.Println("Cannot query: ", err.Error())
	}
	defer rows.Close()

	fmt.Println(rows)
	var result string
	l := SpCallLog{
		spCallLogId: uuid.NewV4(),
		spName:      spName,
		params:      params,
		durationEx:  durationEx,
	}
	for rows.Next() {
		err = rows.Scan(&l.dbName, &result, &l.duration, &l.errorCode, &l.errorMessage)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(l)
		}
	}
	return result, l
}
