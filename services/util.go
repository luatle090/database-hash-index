package services

import (
	"fmt"
	"os"
	"time"
)

// file có tồn tại
func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func GenerateFileName() string {
	current := time.Now()
	// current.Hour()
	// current.Minute()
	// current.Second()
	// current.YearDay()
	// current.Year()
	return fmt.Sprintf("%d-file.log", current.Unix())
}
