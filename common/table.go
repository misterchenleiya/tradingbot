package common

import (
	"fmt"
	"strings"
)

func BuildTableLines(columns []string, rows [][]string) ([]string, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("empty columns")
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
	return lines, nil
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
