package dm

import (
	ctx "context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	_ "unsafe"
)

var db *sql.DB
var defaultTxOptions *sql.TxOptions = &sql.TxOptions{
	Isolation: sql.LevelSerializable,
}

// Error returned if some field of a struct is not addressable.
var FieldNotAddressableErr = errors.New("field not addressable")

// Set the database to use.
func UseDB(d *sql.DB) {
	db = d
}

// Check database connection.
func Ping() error {
	return db.PingContext(ctx.Background())
}

// Create a new transaction.
func NewTx(c ctx.Context) (*sql.Tx, error) {
	return db.BeginTx(c, defaultTxOptions)
}

// Execute SQL statements in query q.
func Exec(c ctx.Context, q string, a ...any) (sql.Result, error) {
	return ExecTx(c, nil, q, a...)
}

// Transaction version of Exec.
func ExecTx(c ctx.Context, t *sql.Tx, q string, a ...any) (sql.Result, error) {
	if t != nil {
		return t.ExecContext(c, q, a...)
	}
	return db.ExecContext(c, q, a...)
}

// Get the first row mapped to type T from query q. If the query
// returns more than one row only the first row is mapped and the
// remaining rows are ignored.
func One[T any](c ctx.Context, q string, a ...any) (T, error) {
	return OneTx[T](c, nil, q, a...)
}

// Transaction version of One.
func OneTx[T any](c ctx.Context, t *sql.Tx, q string, a ...any) (T, error) {
	var obj T
	objs, err := qtx[T](c, t, true, q, a...)
	if err != nil {
		return obj, err
	} else if len(objs) > 0 {
		return objs[0], nil
	}
	return obj, sql.ErrNoRows
}

// Get zero or more rows mapped to a slice of type T from query q.
func Query[T any](c ctx.Context, q string, a ...any) ([]T, error) {
	return qtx[T](c, nil, false, q, a...)
}

// Transaction version of Query.
func QueryTx[T any](c ctx.Context, t *sql.Tx, q string, a ...any) ([]T, error) {
	return qtx[T](c, t, false, q, a...)
}

func qtx[T any](c ctx.Context, t *sql.Tx, tr bool, q string, a ...any) ([]T, error) {
	var objs []T
	var err error
	var rows *sql.Rows
	if t == nil {
		rows, err = db.QueryContext(c, q, a...)
	} else {
		rows, err = t.QueryContext(c, q, a...)
	}
	if err != nil {
		return objs, err
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		return objs, err
	}
	// ATTN: we don't actually use the types, do we need them
	// here?
	// types := make([]any, len(cols))
	// for i, t := range cols {
	// 	types[i] = t.ScanType()
	// }
	for rows.Next() {
		var obj T
		ptrs := make([]any, len(cols))
		for i, col := range cols {
			name := col.Name()
			// TODO: name conversion hook

			// TODO: should return a possible error?
			ip := IndexPath[T](name)

			// get a pointer to the named field of obj
			st := reflect.ValueOf(&obj).Elem()
			field, err := st.FieldByIndexErr(ip)
			if err != nil {
				return objs, err
			}
			if !field.CanAddr() {
				return objs, FieldNotAddressableErr
			}
			ptr := field.Addr().UnsafePointer()

			// get the type of the named field as declared
			// in T
			f := reflect.TypeOf(obj).FieldByIndex(ip)
			t := f.Type

			// add the typed pointer to our slice of
			// pointers
			switch t.String() {
			case "[]byte", "[]uint8":
				ptrs[i] = (*[]byte)(ptr)
			case "bool":
				ptrs[i] = (*bool)(ptr)
			case "int":
				ptrs[i] = (*int)(ptr)
			case "int8":
				ptrs[i] = (*int8)(ptr)
			case "int16":
				ptrs[i] = (*int16)(ptr)
			case "int32":
				ptrs[i] = (*int32)(ptr)
			case "int64":
				ptrs[i] = (*int64)(ptr)
			case "uint":
				ptrs[i] = (*uint)(ptr)
			case "uint8":
				ptrs[i] = (*uint8)(ptr)
			case "uint16":
				ptrs[i] = (*uint16)(ptr)
			case "uint32":
				ptrs[i] = (*uint32)(ptr)
			case "uint64":
				ptrs[i] = (*uint64)(ptr)
			case "string":
				ptrs[i] = (*string)(ptr)
			case "float32":
				ptrs[i] = (*float32)(ptr)
			case "float64":
				ptrs[i] = (*float64)(ptr)
			case "sql.NullBool":
				ptrs[i] = (*sql.NullBool)(ptr)
			case "sql.NullByte":
				ptrs[i] = (*sql.NullByte)(ptr)
			case "sql.NullFloat64":
				ptrs[i] = (*sql.NullFloat64)(ptr)
			case "sql.NullInt16":
				ptrs[i] = (*sql.NullInt16)(ptr)
			case "sql.NullInt32":
				ptrs[i] = (*sql.NullInt32)(ptr)
			case "sql.NullInt64":
				ptrs[i] = (*sql.NullInt64)(ptr)
			case "sql.NullString":
				ptrs[i] = (*sql.NullString)(ptr)
			case "sql.NullTime":
				ptrs[i] = (*sql.NullTime)(ptr)
			case "time.Time":
				ptrs[i] = (*time.Time)(ptr)
			default:
				return objs, fmt.Errorf("no conversion for %v", t)
			}
		}
		err := rows.Scan(ptrs...)
		if err != nil {
			return objs, err
		}
		objs = append(objs, obj)
		if tr {
			return objs, nil
		}
	}
	return objs, nil
}

// IndexPath resolves the slice of indexes for the struct type T that
// can be used as an argument to FieldByIndex. IndexPath takes a path
// p that names the nested fields. The fields in the path are
// seperated by ".".
func IndexPath[T any](p string) []int {
	var obj T
	var ip []int
	t := reflect.TypeOf(obj)
	fields := strings.Split(p, ".")
	for _, f := range fields {
		ft, ok := t.FieldByName(f)
		if ok && len(ft.Index) > 0 {
			ip = append(ip, ft.Index[0])
			t = ft.Type
		}
	}
	return ip
}
