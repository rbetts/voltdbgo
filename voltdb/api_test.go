package voltdb

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestCallOnClosedConn(t *testing.T) {
	conn := new(Conn)
	_, err := conn.Call("bad", 1, 2)
	if err == nil {
		t.Errorf("Expected error calling procedure on closed Conn")
	}
}

func TestTableAccessors(t *testing.T) {
	statusCode := 1
	columnCount := 10
	columnTypes := []int8{1, 2, 3}
	columnNames := []string{"abc", "def", "ghi"}
	rowCount := 5
	rows := bytes.NewBufferString("rowbuf")
	table := Table{
		int8(statusCode),
		int16(columnCount),
		columnTypes,
		columnNames,
		int32(rowCount),
		*rows}

	if table.StatusCode() != statusCode {
		t.Errorf("Bad StatusCode()")
	}
	if table.ColumnCount() != columnCount {
		t.Errorf("Bad ColumnCount()")
	}
	if table.ColumnTypes()[0] != columnTypes[0] {
		t.Errorf("Bad ColumnTypes(). Have %v. Expected %v",
			table.ColumnTypes()[0], columnTypes[0])
	}
	if table.ColumnNames()[1] != columnNames[1] {
		t.Errorf("Bad ColumnNames(). Have %v. Expected %v",
			table.ColumnNames()[1], columnNames[1])
	}
	if table.RowCount() != rowCount {
		t.Errorf("Bad RowCount()")
	}
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}

	opErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}

	return opErr.Timeout()
}

// Note: This test takes two minutes unless "go test" are invoked with -short.
func TestCallTimeout(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Unable to start listener: %s", err)
	}
	defer ln.Close()

	errors := make(chan error)
	go func() {
		conn := new(Conn)
		var err error
		conn.tcpConn, err = net.DialTimeout("tcp", ln.Addr().String(), time.Second)
		if err != nil {
			errors <- err
			return
		}

		if testing.Short() {
			conn.SetResponseDeadline(time.Now().Add(time.Second))
		}

		_, err = conn.Call("@AdHoc", "SELECT 1")
		errors <- err
	}()

	select {
	case err := <-errors:
		if !isTimeout(err) {
			t.Errorf("Expected timeout error, got %s", err)
		}
	case <-time.After(callResponseWait + time.Second):
		t.Errorf("Did not time out after expected duration.")
	}
}

func TestDialTimeout(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Unable to start listener: %s", err)
	}
	defer ln.Close()

	errors := make(chan error)
	go func() {
		_, err := NewConnection("", "", "10.255.255.1:21212")
		errors <- err
	}()

	select {
	case err := <-errors:
		if !isTimeout(err) {
			t.Errorf("Expected timeout error, got %s", err)
		}
	case <-time.After(connectWait + time.Second):
		t.Error("NewConnection did not time out after expected duration.")
	}

}

func TestLoginTimeout(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Unable to start listener: %s", err)
	}
	defer ln.Close()

	errors := make(chan error)
	go func() {
		_, err := NewConnection("", "", ln.Addr().String())
		errors <- err
	}()

	// Accept a connection, but don't read/write. Should result in a timeout *net.OpError.
	conn, _ := ln.Accept()
	defer conn.Close()

	select {
	case err := <-errors:
		if !isTimeout(err) {
			t.Errorf("Expected a timeout error, got %s", err)
		}
	case <-time.After(loginResponseWait + time.Second):
		t.Error("NewConnection did not time out after expected duration.")
	}
}
