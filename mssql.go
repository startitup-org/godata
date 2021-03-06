package godata

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/satori/go.uuid"
)

type MsSQL struct {
	appDb    *sql.DB
	logsDb   *sql.DB
	hostname string
	appSem   chan struct{}
	logsSem  chan struct{}
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
	CreatedDT      time.Time `json:"createdDt"`
}

var DbSem = 2

func NewMsSQL(c DbConns) (*MsSQL, error) {
	appDb, err := sql.Open("mssql", c.AppConn)
	if err != nil {
		log.Println("Cannot connect to appDb: ", err.Error())
	}

	logsDb, err := sql.Open("mssql", c.LogsConn)
	if err != nil {
		log.Println("Cannot connect to logsDb: ", err.Error())
	}

	hostname, _ := os.Hostname()

	appSem := make(chan struct{}, DbSem)
	logsSem := make(chan struct{}, DbSem)

	return &MsSQL{appDb, logsDb, hostname, appSem, logsSem}, err
}

func (db *MsSQL) Close() {
	if db.appDb != nil {
		db.appDb.Close()
	}
	if db.logsDb != nil {
		db.logsDb.Close()
	}
}

func (db *MsSQL) CallSp(spName string, params string) (result []byte, l SpCallLog) {
	db.appSem <- struct{}{}
	//log.Printf("EXEC %s '%s'\n", spName, params)
	tm0 := time.Now()
	row := db.appDb.QueryRow("EXEC CallSp ?1, ?2", spName, params)
	durationEx := time.Since(tm0).Seconds() * 1000 //in ms

	//log.Println(row)
	l = SpCallLog{
		SpCallLogId: uuid.NewV4(),
		SpName:      spName,
		Params:      params,
		DurationEx:  durationEx,
		Server:      db.hostname,
		CreatedDT:   tm0.UTC(),
	}
	err := row.Scan(&l.DbName, &result, &l.Duration, &l.ErrorCode, &l.ErrorMessage, &l.ExErrorCode, &l.ExErrorMessage)
	if err != nil {
		log.Println("row.Scan error:", err)
	} else {
		//log.Println("l:", l)
		go db.CallLogSp("LogSpCall", l)
	}
	<-db.appSem
	return
}

func (db *MsSQL) CallLogSp(sp string, l interface{}) (err error, errCode int, errMsg string) {
	db.logsSem <- struct{}{}
	lj, err := json.Marshal(l)
	if err != nil {
		log.Printf("%s json.Marshal %s\n", sp, err)
		return
	}
	//log.Printf("%s IN: %s\n", sp, lj)
	err = db.logsDb.QueryRow("EXEC "+sp+" ?", string(lj)).Scan(&errCode, &errMsg)
	if err != nil {
		log.Printf("%s RES: %s, %d, %s\n", sp, err, errCode, errMsg)
	}
	<-db.logsSem
	return
}

func (db *MsSQL) Ping() error {
	if err := db.appDb.Ping(); err != nil {
		log.Println("MsSQL:Ping appDb error", err)
		return err
	}

	if err := db.logsDb.Ping(); err != nil {
		log.Println("MsSQL:Ping logDb error", err)
		return err
	}

	return nil
}
