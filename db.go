package main

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type docInfo struct {
	name, revision                                               string
	clientUUID, contractorUUID                                   string
	clientProjno, clientDocno, contractorProjno, contractorDocno int
}

type myError struct {
	When time.Time
	What string
}

const dbName string = "dds.db"
const timeoutAttempts = 2

var (
	db  *sql.DB
	err error
)

func (e myError) Error() string {
	return fmt.Sprintf("%v: %v", e.When, e.What)
}

func openConnection() error {
	if db, err = sql.Open("sqlite3", dbName); err != nil {
		return err
	}
	return db.Ping()
}

func checkConnectionNTimes(trys int) error {
	if db != nil && db.Ping() != nil {
		return nil
	} else if openConnection() != nil {
		time.Sleep(10 * time.Second)
		if trys > 0 {
			checkConnectionNTimes(trys - 1)
		} else {
			return myError{time.Now(), `Failed to connect to database, 
				service timed out after ` + strconv.Itoa(timeoutAttempts) + ` times`}
		}
	}
	return nil
}

func checkConnection() {
	checkConnectionNTimes(timeoutAttempts)
}

func countRows() {
	checkConnection()
	count := 0
	err = db.QueryRow(`select count(*) from doc_switch`).Scan(&count)
	fmt.Println(count)
}

func selectDoc(guidIn string) (docInfo, error) {
	checkConnection()
	var d docInfo
	query := `select * from doc_switch where (contractor_guid = '` + guidIn + `'
						OR client_guid = '` + guidIn + `')`
	err = db.QueryRow(query).Scan(&d.name, &d.revision, &d.clientUUID, &d.clientProjno, &d.clientDocno,
		&d.contractorUUID, &d.contractorProjno, &d.contractorDocno)
	return d, err
}

func insertTestRows(start int, count int, wg *sync.WaitGroup) (rowsInserted int64) {
	defer wg.Done()
	dbs, errs := sql.Open("sqlite3", dbName)
	if err != nil {
		return
	}
	dbs.SetMaxOpenConns(1)
	dbs.Exec("PRAGMA journal_mode=WAL")
	dbs.Exec("BEGIN TRANSACTION")
	defer dbs.Exec("END TRANSACTION")

	fstr := `INSERT INTO doc_switch (name, revision,
		client_guid, client_projno,  client_docno, contractor_guid,
		contractor_projno, contractor_docno)
		VALUES('doc0%v','P01', 'doc%vclientguid',
		12345, %v, 'doc%vcontractguid', 543321, %v)`
	for i := start; i <= count; i++ {
		stmt := fmt.Sprintf(fstr, i, i, i, i, i)
		if _, errs = dbs.Exec(stmt); errs != nil {
			if errs.Error() == "database is locked" {
				i--
			} else {
				//log it
			}
		} else {
			rowsInserted++
		}
	}
	return
}

func insertConcurrent(threads int, count int) {
	deleteAllRecords()
	split := count / threads
	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go insertTestRows(i*split, ((i*split)+split)-1, &wg)
	}
	wg.Wait()
	fmt.Println("done")
}

func deleteAllRecords() (rowsDeleted int64) {
	checkConnection()
	query := `delete from doc_switch`
	res, err := db.Exec(query)
	if err != nil {

		log.Fatal(err)
	}
	rowsDeleted, _ = res.RowsAffected()
	return
}

func initDb() {
	checkConnection()
	stmt := `create table doc_switch (
		name text not null,
		revision text not null,
		client_guid text not null, 
		client_projno INTEGER not null, 
		client_docno INTEGER not null,
		contractor_guid text not null, 
		contractor_projno INTEGER not null, 
		contractor_docno INTEGER not null,		
		CONSTRAINT name_rev UNIQUE(name, revision));`
	if _, err = db.Exec(stmt); err != nil {
		//logit
	}
	stmt = `CREATE INDEX clientguid_index 
					ON doc_switch (client_guid)`
	if _, err = db.Exec(stmt); err != nil {
		//logit
	}
	stmt = `CREATE INDEX contractorguid_index 
					ON doc_switch (contractor_guid)`
	if _, err = db.Exec(stmt); err != nil {
		//logit
	}
}

func resetDb() {
	checkConnection()
	stmt := `drop table doc_switch`
	db.Exec(stmt)
	initDb()
}
