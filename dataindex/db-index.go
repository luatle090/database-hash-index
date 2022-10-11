package dataindex

import (
	"bufio"
	"fmt"
	"github/database-hash-index/services"
	"os"
	"strings"
)

type DataEntity struct {
	Offset int
	Key    string
	Value  string
}

type Database struct {
	file *os.File
	data map[string]DataEntity
	path string
}

var nextOffset int = 0

var moveCursorToNext = 1

func Open(path string) (*Database, error) {
	var file *os.File
	var err error
	if !services.FileExists(path) {
		file, err = os.Create(path)
	} else {
		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0666)
	}

	if err != nil {
		return nil, err
	}
	db := &Database{
		file: file,
		path: path,
	}
	if err = loadIndex(db); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *Database) Close() error {
	return db.file.Close()
}

// set dữ liệu vào log theo key và value
func (db *Database) DB_Set(key, value string) error {
	n, err := fmt.Fprintf(db.file, "%s,%s\n", key, value)
	if err != nil {
		return err
	}

	db.hashIndex(key, value, n)
	return nil

}

// lấy dữ liệu từ log theo key
func (db *Database) DB_Get(key string) (string, error) {
	data, err := db.get(key)
	if err != nil {
		return "", err
	}
	// file.Seek(int64(offset), 0)
	// scanner := bufio.NewScanner(file)
	// var valueFromLastKey string

	// if scanner.Scan() {
	// 	str := strings.Split(scanner.Text(), ",")
	// 	valueFromLastKey = str[1]
	// }
	// return valueFromLastKey, nil
	return data.Value, nil

}

func (db *Database) DB_Compaction() error {
	return db.db_compaction()
}

// load index có sẵn
func loadIndex(db *Database) error {
	offsets := make(map[string]DataEntity)
	var n int
	file, err := os.Open(db.path)
	if err != nil {
		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		str := scanner.Text()
		byte := len(str)

		prop := strings.Split(str, ",")
		offsets[prop[0]] = DataEntity{Offset: n, Key: prop[0], Value: prop[1]}
		n += byte + moveCursorToNext

	}
	info, _ := db.file.Stat()

	nextOffset = int(info.Size())
	db.data = offsets
	fmt.Println()
	fmt.Println(offsets)
	return nil
}

// feature Compaction
func (db *Database) db_compaction() error {
	file, err := os.Create(services.GenerateFileName())
	if err != nil {
		return err
	}
	for _, data := range db.data {
		_, err := fmt.Fprintf(file, "%s,%s\n", data.Key, data.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

// set key và offset vào map data
func (db *Database) hashIndex(key, value string, n int) {
	db.data[key] = DataEntity{Offset: nextOffset, Key: key, Value: value}
	nextOffset += n
	fmt.Println(db.data)
}

// lấy Data Entity từ trong map data
func (db *Database) get(key string) (DataEntity, error) {
	offset, ok := db.data[key]
	if !ok {
		return DataEntity{}, fmt.Errorf("not found key %s", key)
	}
	return offset, nil
}

func (dt DataEntity) String() string {
	return fmt.Sprintf("data %s offset - %d\n", dt.Value, dt.Offset)
}

// func newDataEntity(key, value string) DataEntity {
// 	return DataEntity{
// 		Value: value,
// 		Key:   key,
// 	}
// }
