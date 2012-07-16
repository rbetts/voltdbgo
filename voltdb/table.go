package voltdbgo

import (
	"fmt"
)

// Table represents a VoltDB table, often as a procedure result set.
// Wrap up some metdata with pointer(s) to row data. Tables are
// relatively cheap to copy (the associated user data is copied
// reference).
type Table struct {
	statusCode  int8
	columnCount int16
	columnTypes []int8
	columnNames []string
	rowCount    int32
	rows        []Row
}

func (table Table) GoString() string {
	return fmt.Sprintf("Table: statusCode: %v, columnCount: %v, "+
		"rowCount: %v\n", table.statusCode, table.columnCount,
		table.rowCount)
}

type Row struct {
	vals []interface{}
}
