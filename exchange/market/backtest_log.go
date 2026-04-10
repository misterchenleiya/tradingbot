package market

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const backTestCSVTimeLayout = "2006-01-02 15:04:05"

func printBackTestCSVSources(files []CSVFileSpec) {
	if len(files) == 0 {
		return
	}
	timeRangeColumn := fmt.Sprintf("time_range(%s)", backTestLocalTimeLabel())
	rows := make([][]string, 0, len(files))
	for _, spec := range files {
		timeRange := formatBackTestCSVTimeRangeLocal(spec.Start, spec.End)
		fileName := spec.DisplayFile
		filePath := spec.DisplayDir
		if fileName == "" {
			fileName = filepath.Base(spec.Path)
			if fileName == "." {
				fileName = ""
			}
		}
		rows = append(rows, []string{
			spec.Exchange,
			spec.Symbol,
			spec.Timeframe,
			timeRange,
			filePath,
			fileName,
		})
	}
	lines := buildTableLines([]string{"exchange", "symbol", "timeframe", timeRangeColumn, "file_path", "file"}, rows)
	lines = append([]string{"[back-test csv sources]"}, lines...)
	printLines(lines)
}

func formatBackTestCSVTimeRangeLocal(start, end time.Time) string {
	if start.IsZero() || end.IsZero() {
		return ""
	}
	return fmt.Sprintf("%s ~ %s", formatBackTestCSVTimeLocal(start), formatBackTestCSVTimeLocal(end))
}

func formatBackTestCSVTimeLocal(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.In(time.Local).Format(backTestCSVTimeLayout)
}

func backTestLocalTimeLabel() string {
	now := time.Now().In(time.Local)
	name, offset := now.Zone()
	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	hour := offset / 3600
	minute := (offset % 3600) / 60
	return fmt.Sprintf("%s%s%02d:%02d", name, sign, hour, minute)
}

func buildTableLines(columns []string, rows [][]string) []string {
	if len(columns) == 0 {
		return nil
	}
	normalized := make([][]string, len(rows))
	for i, row := range rows {
		fixed := make([]string, len(columns))
		copy(fixed, row)
		normalized[i] = fixed
	}

	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}
	for _, row := range normalized {
		for i, val := range row {
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	lines := make([]string, 0, len(normalized)+4)
	border := formatBorder(widths)
	lines = append(lines, border)
	lines = append(lines, formatRow(columns, widths))
	lines = append(lines, border)
	for _, row := range normalized {
		lines = append(lines, formatRow(row, widths))
	}
	lines = append(lines, border)
	return lines
}

func formatBorder(widths []int) string {
	var b strings.Builder
	b.WriteString("+")
	for _, w := range widths {
		b.WriteString(strings.Repeat("-", w+2))
		b.WriteString("+")
	}
	return b.String()
}

func formatRow(values []string, widths []int) string {
	var b strings.Builder
	b.WriteString("|")
	for i, val := range values {
		b.WriteString(" ")
		b.WriteString(val)
		if padding := widths[i] - len(val); padding > 0 {
			b.WriteString(strings.Repeat(" ", padding))
		}
		b.WriteString(" |")
	}
	return b.String()
}

func printLines(lines []string) {
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			fmt.Println()
			continue
		}
		fmt.Println(line)
	}
}
