package main

import (
	"flag"
	"fmt"
)

const str string = "Hello"

var flagTest = flag.String("dbname", "db", "a string")

func main() {

	//initDb()
	//resetDb()
	//insertTestRows(0, x1000000)

	//insertConcurrent(8, 10000000)
	//insertConcurrent(1, 10000000)
	//countRows()

	a, err := selectDoc("doc4436587clientguidx")
	if err != nil {
		fmt.Println("not found")
	}
	fmt.Println(a)
	//fmt.Println(deleteAllRecords())
	// if docExists("doc3clientguid") {
	// 	fmt.Println("Doc found")
	// } else {
	// 	fmt.Println("Doc not found")
	// }

	// for i := 0; i < 10; i++ {

	// 	time.Sleep(time.Second * 3)
	// }
}
