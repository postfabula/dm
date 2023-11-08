package tests_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
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

func TestGetOne(t *testing.T) {
	setupAlbums(t)
	album, err := dm.One[Album](context.Background(),
		`SELECT * FROM "Albums" WHERE ID = 1;`)
	if err != nil {
		t.Fatalf("❌ %q", err)
	}
	if k := reflect.TypeOf(album).Kind(); k != reflect.Struct {
		t.Fatalf("❌ expected struct, go %q ", k)
	}
	if album.Title != "Eliminator" {
		t.Fatalf("❌ expected Title == Eliminator")
	}
	teardownAlbums(t)
}

type User struct {
	ID   int64
	Name string
}
type Message struct {
	ID   int64
	User User
	Body string
}

func setupMessages(t *testing.T) {
	_, err := dm.Exec(context.Background(), `
DROP TABLE IF EXISTS Messages;
CREATE TABLE Messages (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  UserID INTEGER,
  Body VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS Users;
CREATE TABLE Users (
  ID INTEGER PRIMARY KEY AUTOINCREMENT,
  Name VARCHAR(128) NOT NULL
);

INSERT INTO Users
  (Name)
VALUES
  ('Stephen Duffy'),
  ('Curt Smith'),
  ('Neil Tennant');

INSERT INTO Messages
  (UserID, Body)
VALUES
  (1, 'Dark in the city night is a wire.'),
  (1, 'Steam in the subway earth is afire.'),
  (2, 'I wanted to be with you alone.'),
  (2, 'And talk about the weather.'),
  (3, 'Sometimes you''re better off dead.'),
  (3, 'There''s a gun in your hand it''s pointing at your head.');`)
	if err != nil {
		t.Fatal(err)
	}
}

func teardownMessages(t *testing.T) {
	_, err := dm.Exec(context.Background(),`
DROP TABLE IF EXISTS Messages;
DROP TABLE IF EXISTS Users;`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddedMapping(t *testing.T) {
	setupMessages(t)
	messages, err := dm.Query[Message](context.Background(),`
SELECT
  m.ID,
  m.Body,
  u.ID AS "User.ID",
  u.Name AS "User.Name"
FROM Messages m
JOIN Users u
ON m.UserID = u.ID;`)
	if err != nil {
		t.Fatalf("❌ %q", err)
	}
	if len(messages) != 6 {
		t.Fatalf("❌ expected 6 messages")
	}
	if messages[2].User.Name != "Curt Smith" {
		t.Fatalf("❌ expected User.Name == Curt Smith")
	}
	teardownMessages(t)
}
