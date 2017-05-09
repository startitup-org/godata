package godata

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
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
	SpCallLogId    uuid.UUID `json:"spCallLogId"`
	ApiAccessLogId uuid.UUID `json:"apiAccessLogId"`
	DbName         string    `json:"dbName"`
	SpName         string    `json:"spName"`
	Params         string    `json:"params"`
	Duration       float64   `json:"duration"`
	DurationEx     float64   `json:"durationEx"`
	ErrorCode      int       `json:"errorCode"`
	ErrorMessage   string    `json:"errorMessage"`
	ExErrorCode    int       `json:"exErrorCode"`
	ExErrorMessage string    `json:"exErrorMessage"`
	Server         string    `json:"server"`
}

var hostname string

func NewMsSQL(c DbConns) (*MsSQL, error) {
	appDb, err := sql.Open("mssql", c.AppConn)
	if err != nil {
		fmt.Println("Cannot connect to appDb: ", err.Error())
	}

	logsDb, err := sql.Open("mssql", c.LogsConn)
	if err != nil {
		fmt.Println("Cannot connect to logsDb: ", err.Error())
	}

	hostname, _ = os.Hostname()

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
	//fmt.Printf("EXEC %s '%s'\n", spName, params)
	tm0 := time.Now()
	row := db.appDb.QueryRow("EXEC CallSp ?1, ?2", spName, params)
	durationEx := time.Since(tm0).Seconds() * 1000 //in ms

	//fmt.Println(row)
	var result string
	l := SpCallLog{
		SpCallLogId: uuid.NewV4(),
		SpName:      spName,
		Params:      params,
		DurationEx:  durationEx,
		Server:      hostname,
	}
	err := row.Scan(&l.DbName, &result, &l.Duration, &l.ErrorCode, &l.ErrorMessage, &l.ExErrorCode, &l.ExErrorMessage)
	if err != nil {
		fmt.Println("row.Scan error:", err)
	} else {
		//fmt.Println("l:", l)
		go db.LogSpCall(l)
	}
	return result, l
}

func (db *MsSQL) LogSpCall(l SpCallLog) {
	lj, err := json.Marshal(l)
	if err != nil {
		fmt.Println(err)
	}

	db.logsDb.QueryRow("EXEC LogSpCall ?", string(lj))
}
