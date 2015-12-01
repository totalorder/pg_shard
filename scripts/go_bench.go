package main

import (
	"fmt"
	"log"
	"time"
	"math/rand"
	"database/sql"
	_ "github.com/lib/pq"
)


const totalQueries = 400


// func measureNormalTime() {
// 	durations := make(chan time.Duration)

//     for i := 0; i < totalQueries; i++ {
//     	go func() error {
// 			db, err := sql.Open("postgres", "user=postgres dbname=shard sslmode=disable host=192.168.99.100 port=5433")
// 			if err != nil {
// 				return err
// 			}
// 			defer db.Close()

// 			start := time.Now()

// 			if err != nil {
// 				return err
// 			}

// 			rand_id := rand.Intn(23776863) + 1
// 			rows, err := db.Query("SELECT id, username FROM usr WHERE id = $1 OR username IN ('a','b','c','d','e','f','g','h','i','j')", rand_id)

// 			if err != nil {
// 				return err
// 			}

// 			defer rows.Close()

// 			for rows.Next() {
// 			 	var id int
// 				var username string
// 			    err = rows.Scan(&id, &username)

// 			    if err != nil {
// 					return err
// 				}
// 			}

// 			err = rows.Err()

// 			durations <- time.Since(start)
// 			return err
// 		}()
//     }

// 	var totalDuration time.Duration = 0

//     for i := 0; i < totalQueries; i++ {
//         totalDuration += <-durations
//     }

//     fmt.Println("Avg query time:", totalDuration / totalQueries)
// }


func measureShardedTime() {
	durations := make(chan time.Duration)
	errors := make(chan error)

    for i := 0; i < totalQueries; i++ {
    	db, err := sql.Open("postgres", "user=postgres dbname=shard sslmode=disable host=192.168.99.100 port=5434")
		if err != nil {
		    errors <- err
		    return
		}
		defer db.Close()

    	go func(db *sql.DB) {
			start := time.Now()

			rand_id := rand.Intn(23776863) + 1
			tx, err := db.Begin()

			if err != nil {
			    errors <- err
			    return
			}

			defer tx.Rollback()
			_, err = tx.Exec("SELECT shard('userid'::text, $1::int)", rand_id)

			if err != nil {
			    errors <- err
			    return
			}

			rows, err := tx.Query(fmt.Sprintf("SELECT id, username FROM usr WHERE id = %d OR username IN ('a','b','c','d','e','f','g','h','i','j')", rand_id))
			if err != nil {
			    errors <- err
			    return
			}

			defer rows.Close()

			for rows.Next() {
			 	var id int
				var username string
			    err = rows.Scan(&id, &username)

			    if err != nil {
			    	errors <- err
			    	return
				}
			}

			err = rows.Err()
			if err != nil {
			    errors <- err
			    return
			}

			tx.Commit()
			durations <- time.Since(start)
		}(db)
    }

	var totalDuration time.Duration = 0
	var d time.Duration
	var err error

    for i := 0; i < totalQueries; i++ {
    	select {
			case d = (<-durations):
				totalDuration += d
			case err = (<-errors):
				log.Fatal(err)
		}
    }

    fmt.Println("Avg query time:", totalDuration / totalQueries)
}


func main() {
	// measureNormalTime()
	measureShardedTime()
}
