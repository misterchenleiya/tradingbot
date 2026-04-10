package market

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func readCSV(path string) ([]models.OHLCV, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	var out []models.OHLCV
	rowIndex := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(record) == 0 {
			rowIndex++
			continue
		}
		if rowIndex == 0 && isCSVHeader(record) {
			rowIndex++
			continue
		}
		if len(record) < 6 {
			return nil, fmt.Errorf("invalid csv row %d", rowIndex+1)
		}
		ts, err := parseCSVInt(record[0])
		if err != nil {
			return nil, fmt.Errorf("invalid ts at row %d: %w", rowIndex+1, err)
		}
		open, err := parseCSVFloat(record[1])
		if err != nil {
			return nil, fmt.Errorf("invalid open at row %d: %w", rowIndex+1, err)
		}
		high, err := parseCSVFloat(record[2])
		if err != nil {
			return nil, fmt.Errorf("invalid high at row %d: %w", rowIndex+1, err)
		}
		low, err := parseCSVFloat(record[3])
		if err != nil {
			return nil, fmt.Errorf("invalid low at row %d: %w", rowIndex+1, err)
		}
		closePx, err := parseCSVFloat(record[4])
		if err != nil {
			return nil, fmt.Errorf("invalid close at row %d: %w", rowIndex+1, err)
		}
		volume, err := parseCSVFloat(record[5])
		if err != nil {
			return nil, fmt.Errorf("invalid volume at row %d: %w", rowIndex+1, err)
		}
		out = append(out, models.OHLCV{
			TS:     ts,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closePx,
			Volume: volume,
		})
		rowIndex++
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("empty csv data")
	}
	return out, nil
}

func writeCSV(path string, data []models.OHLCV) error {
	if len(data) == 0 {
		return fmt.Errorf("empty csv data")
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"ts", "open", "high", "low", "close", "volume"}); err != nil {
		return err
	}
	for _, item := range data {
		row := []string{
			strconv.FormatInt(item.TS, 10),
			formatCSVFloat(item.Open),
			formatCSVFloat(item.High),
			formatCSVFloat(item.Low),
			formatCSVFloat(item.Close),
			formatCSVFloat(item.Volume),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func isCSVHeader(record []string) bool {
	if len(record) == 0 {
		return false
	}
	first := strings.ToLower(strings.TrimSpace(record[0]))
	if first == "ts" || first == "timestamp" {
		return true
	}
	_, err := strconv.ParseInt(strings.TrimSpace(record[0]), 10, 64)
	return err != nil
}

func parseCSVInt(raw string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
}

func parseCSVFloat(raw string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(raw), 64)
}

func formatCSVFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
