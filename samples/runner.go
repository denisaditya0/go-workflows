package samples

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/postgres"
	"github.com/cschleiden/go-workflows/diag"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Connection defaults for the local docker-compose Postgres instance.
const (
	pgHost     = "localhost"
	pgPort     = 5432
	pgUser     = "root"
	pgPassword = "root"
)

// GetBackend returns a Postgres-backed backend for the samples.
//
// A dedicated database named after the sample is created (dropped and recreated
// when recreate is true) so each sample runs in isolation. A diagnostics UI is
// served at http://localhost:3000/diag.
func GetBackend(name string, recreate bool, opt ...backend.BackendOption) backend.Backend {
	flag.Parse()

	adminDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable", pgHost, pgPort, pgUser, pgPassword)
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if recreate {
		if _, err := db.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)"); err != nil {
			panic(fmt.Errorf("dropping database: %w", err))
		}
	}

	// CREATE DATABASE fails if it already exists; ignore that error so repeated
	// runs without recreate still work.
	if _, err := db.Exec("CREATE DATABASE " + name); err != nil {
		log.Printf("note: could not create database %q (it may already exist): %v", name, err)
	}

	b := postgres.NewPostgresBackend(pgHost, pgPort, pgUser, pgPassword, name, postgres.WithBackendOptions(opt...))

	// Start diagnostic server under /diag
	m := http.NewServeMux()
	m.Handle("/diag/", http.StripPrefix("/diag", diag.NewServeMux(b)))
	go http.ListenAndServe(":3000", m)

	log.Println("Debug UI available at http://localhost:3000/diag")

	return b
}
