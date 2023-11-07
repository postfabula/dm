package tests_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/postfabula/dm"
	//_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

func init() {
	db, err := sql.Open("sqlite", "test.sqlitedb")
	if err != nil {
		fmt.Println(err)
	}
	dm.UseDB(db)
}

type Album struct {
	ID     int64
	Title  string
	Artist string
}

func setupAlbums(t *testing.T) {
	_, err := dm.Exec(context.Background(), `
DROP TABLE IF EXISTS Albums;
CREATE TABLE Albums (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Title VARCHAR(128) NOT NULL,
  Artist VARCHAR(255) NOT NULL
);

INSERT INTO Albums
  (Title, Artist)
VALUES
  ('Eliminator', 'ZZ Top'),
  ('Sports', 'Huey Lewis and The News'),
  ('Songs from the Big Chair', 'Tears For Fears'),
  ('Violator', 'Depeche Mode');`)
	if err != nil {
		t.Fatal(err)
	}
}

func teardownAlbums(t *testing.T) {
	_, err := dm.Exec(context.Background(),
		`DROP TABLE IF EXISTS Albums;`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBasicMapping(t *testing.T) {
	setupAlbums(t)
	albums, err := dm.Query[Album](context.Background(),
		`SELECT * FROM "Albums";`)
	if err != nil {
		t.Fatalf("❌ %q", err)
	}
	if len(albums) != 4 {
		t.Fatalf("❌ expected 4 albums")
	}
	teardownAlbums(t)
}
