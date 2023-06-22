// db.go

package kaspastratum
import (
    "fmt"
    "database/sql"
    _ "github.com/lib/pq"
)
var DB *sql.DB
func InitConnection() error {

    if DB != nil {
        return nil // соединение уже установлено
    }
    //    connectionString := "host=localhost port=5432 user=gorbaniov password=1 dbname=gor sslmode=disable"
    connectionString := "host=localhost port=5432 user=gorbaniov password=1 dbname=gor sslmode=require"

    db, err := sql.Open("postgres", connectionString)
    if err != nil {
        return err
    }
    err = db.Ping()
    if err != nil {
        return err
    }
    DB = db
    return nil
}

func GetDB() (*sql.DB, error) {
    if DB == nil {
        return nil, fmt.Errorf("connection not initialized")
    }
    return DB, nil
}
