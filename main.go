package main

import (
	"bufio"
	"fmt"
	"github/database-hash-index/dataindex"
	"log"
	"os"
	"strings"
	"time"
)

const LOG_FILE = "log2.log"

// var vFlag = flag.Bool("set", false, "Set key and value into database")

func main() {
	// flag.Parse()
	// roots := flag.Args()
	// if len(roots) == 0 {
	// 	fmt.Println("input is required")
	// 	return
	// }

	// if *vFlag {
	// 	db_set(roots[0], roots[1])
	// 	return
	// }

	// value, err := db_get(roots[0])
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// fmt.Println(value)

	var input string

	db, err := dataindex.Open(LOG_FILE)

	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	for {
		fmt.Println("Press command key value: ")
		scan := bufio.NewScanner(os.Stdin)
		if scan.Scan() {
			input = scan.Text()
		}
		if input == "exit" {
			break
		}

		token := strings.SplitN(input, " ", 3)
		if len(token) < 2 {
			continue
		}

		if token[0] == "set" {
			db.DB_Set(token[1], token[2])
			continue
		}

		if token[0] == "get" {
			value, err := db.DB_Get(token[1])
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println(value)
		}

		if token[0] == "merge" {
			if err := db.DB_Compaction(); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func DateSub() {
	t1, err := time.Parse("2006-1-2", "2022-10-11")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(t1.Add(time.Hour * 1968))

	t2, err := time.Parse("2006-1-2", "2023-1-1")

	if err != nil {
		fmt.Println(err)
		return
	}

	t3 := t2.Sub(t1)
	fmt.Println(t3.Hours() / 24)
}
