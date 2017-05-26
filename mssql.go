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
	CreatedDT      time.Time `json:"createdDT"`
}

type ApiAccessLog struct {
	ApiAccessLogId uuid.UUID `json:"apiAccessLogId"`
	ApiSessionId   uuid.UUID `json:"apiSessionId"`
	Route          string    `json:"route"`
	Source         string    `json:"source"`
	UserId         uuid.UUID `json:"userId"`
	IPAddress      string    `json:"ipAddress"`
	UserAgent      string    `json:"userAgent"`
	Method         string    `json:"method"`
	Url            string    `json:"url"`
	Headers        string    `json:"headers"`
	Payload        string    `json:"payload"`
	Response       string    `json:"response"`
	ReasonPhrase   string    `json:"reasonPhrase"`
	StatusCode     int       `json:"statusCode"`
	Duration       float64   `json:"duration"`
	Domain         string    `json:"domain"`
	Server         string    `json:"server"`
	CreatedDT      time.Time `json:"createdDT"`
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

func (db *MsSQL) CallSp(spName string, params string) ([]byte, SpCallLog) {
	//fmt.Printf("EXEC %s '%s'\n", spName, params)
	tm0 := time.Now()
	row := db.appDb.QueryRow("EXEC CallSp ?1, ?2", spName, params)
	durationEx := time.Since(tm0).Seconds() * 1000 //in ms

	//fmt.Println(row)
	var result []byte
	l := SpCallLog{
		SpCallLogId: uuid.NewV4(),
		SpName:      spName,
		Params:      params,
		DurationEx:  durationEx,
		Server:      hostname,
		CreatedDT:   tm0.UTC(),
	}
	err := row.Scan(&l.DbName, &result, &l.Duration, &l.ErrorCode, &l.ErrorMessage, &l.ExErrorCode, &l.ExErrorMessage)
	if err != nil {
		fmt.Println("row.Scan error:", err)
	} else {
		//fmt.Println("l:", l)
		go db.logSpCall(l)
	}
	return result, l
}

func (db *MsSQL) logSpCall(l SpCallLog) {
	var errCode int
	var errMsg string

	lj, err := json.Marshal(l)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("logSpCall IN: %s\n", lj)

	err = db.logsDb.QueryRow("EXEC LogSpCall ?", string(lj)).Scan(&errCode, &errMsg)
	if err != nil {
		fmt.Printf("logSpCall RES: %s, %d, %s\n", err, errCode, errMsg)
	}
}

func (db *MsSQL) LogApiAccess(l ApiAccessLog) {
	var errCode int
	var errMsg string

	l.Duration = time.Since(l.CreatedDT).Seconds() * 1000 //in ms
	lj, err := json.Marshal(l)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("LogApiAccess IN: %s\n", lj)

	err = db.logsDb.QueryRow("EXEC LogApiAccess ?", string(lj)).Scan(&errCode, &errMsg)
	if err != nil {
		fmt.Printf("LogApiAccess RES: %s, %d, %s\n", err, errCode, errMsg)
	}
}
