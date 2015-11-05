package main

import (
	"fmt"
	"log"
	"time"
	"math/rand"
	"database/sql"
	_ "github.com/lib/pq"
)

func query() error {
	db, err := sql.Open("postgres", "user=postgres dbname=shard sslmode=disable host=192.168.99.100 port=5433")
	if err != nil {
		fmt.Println("Oops")
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()

	if err != nil {
		return err
	}

	defer tx.Rollback()

	rand_id := rand.Intn(23776863) + 1
	rows, err := tx.Query("SELECT id, username FROM usr WHERE id = $1 OR username IN ('a','b','c','d','e','f','g','h','i','j')", rand_id)

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
	 	var id int
		var username string
	    err = rows.Scan(&id, &username)

	    if err != nil {
			return err
		}
	}


	err = rows.Err()
	tx.Commit()
	return err
}

func tx() error {
	db, err := sql.Open("postgres", "user=postgres dbname=shard sslmode=disable host=192.168.99.100 port=5434")
	if err != nil {
		fmt.Println("Oops")
		log.Fatal(err)
	}
	defer db.Close()

	rand_id := rand.Intn(23776863) + 1
	tx, err := db.Begin()

	if err != nil {
		return err
	}

	defer tx.Rollback()
	_, err = tx.Exec("SELECT shard('userid'::text, $1::int)", rand_id)

	if err != nil {
		return err
	}

	rows, err := tx.Query(fmt.Sprintf("SELECT id, username FROM usr WHERE id = %d OR username IN ('a','b','c','d','e','f','g','h','i','j')", rand_id))

	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
	 	var id int
		var username string
	    err = rows.Scan(&id, &username)

	    if err != nil {
			return err
		}
	}


	err = rows.Err()
	tx.Commit()
	return err
}

func measureTime(f func() error) {
	const totalQueries = 100

	start := time.Now()

    for i := 0; i < totalQueries; i++ {
    	if err := f(); err != nil {
    		log.Fatal(err)
    	}
    }

    elapsed := time.Since(start)
    fmt.Println("Query avg", elapsed / totalQueries)
}

func main() {
	measureTime(query)
	measureTime(tx)
}
