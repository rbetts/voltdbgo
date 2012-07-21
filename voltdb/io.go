package voltdbgo

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
)

// writeMessage prepends a header and writes header and buf to tcpConn
// Table represents a VoltDB table, often as a procedure result set.
// Wrap up some metdata with pointer(s) to row data. Tables are
// relatively cheap to copy (the associated user data is copied
// reference).
func (conn *Conn) writeMessage(buf bytes.Buffer) error {
	length := buf.Len() + 1 // length includes proto version.
	var hdr bytes.Buffer
	writeInt(&hdr, int32(length))
	writeProtoVersion(&hdr)
	io.Copy(conn.tcpConn, &hdr)
	io.Copy(conn.tcpConn, &buf)
	return nil // TODO: obviously wrong
}

// readMessageHdr reads the standard wireprotocol header.
func (conn *Conn) readMessageHdr() (size int32, err error) {
	// Total message length Integer  4
	size, err = readInt(conn.tcpConn)
	if err != nil {
		return
	}
	// Version Byte 1
	// TODO: error on incorrect version.
	_, err = readByte(conn.tcpConn)

	// size includes the protocol version.
	return (size - 1), nil
}

// readLoginResponse parses the login response message.
func (conn *Conn) readMessage() (*bytes.Buffer, error) {
	size, err := conn.readMessageHdr()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if _, err = io.ReadFull(conn.tcpConn, data); err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	return buf, nil
}

func serializeLoginMessage(user string, passwd string) (msg bytes.Buffer, err error) {
	h := sha1.New()
	io.WriteString(h, passwd)
	shabytes := h.Sum(nil)

	err = writeString(&msg, "database")
	if err != nil {
		return
	}
	err = writeString(&msg, user)
	if err != nil {
		return
	}
	err = writeByteString(&msg, shabytes)
	if err != nil {
		return
	}
	return
}

func (conn *Conn) readLoginResponse() (*connectionData, error) {
	buf, err := conn.readMessage()
	if err != nil {
		return nil, err
	}
	connData, err := deserializeLoginResponse(buf)
	return connData, err
}

// configures conn with server's advertisement.
func deserializeLoginResponse(r io.Reader) (connData *connectionData, err error) {
	// Authentication result code	Byte	 1	 Basic
	// Server Host ID	            Integer	 4	 Basic
	// Connection ID	            Long	 8	 Basic
	// Cluster start timestamp  	Long	 8	 Basic
	// Leader IPV4 address	        Integer	 4	 Basic
	// Build string	 String	        variable	 Basic
	ok, err := readByte(r)
	if err != nil {
		return
	}
	if ok != 0 {
		return nil, errors.New("Authentication failed.")
	}

	hostId, err := readInt(r)
	if err != nil {
		return
	}

	connId, err := readLong(r)
	if err != nil {
		return
	}

	_, err = readLong(r)
	if err != nil {
		return
	}

	leaderAddr, err := readInt(r)
	if err != nil {
		return
	}

	buildString, err := readString(r)
	if err != nil {
		return
	}

	connData = new(connectionData)
	connData.hostId = hostId
	connData.connId = connId
	connData.leaderAddr = leaderAddr
	connData.buildString = buildString
	return connData, nil
}

func serializeCall(proc string, ud int64, params []interface{}) (msg bytes.Buffer, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

    if err = writeString(&msg, proc); err != nil {
		return
	}
	if err = writeLong(&msg, ud); err != nil {
		return
	}

	serializedParams, err := serializeParams(params)
	if err != nil {
		return
	}
	io.Copy(&msg, &serializedParams)
	return
}

func serializeParams(params []interface{}) (msg bytes.Buffer, err error) {
	// parameter count      short
	// (type byte, parameter)*
	if err = writeShort(&msg, int16(len(params))); err != nil {
		return
	}
	for _, val := range params {
		if err = marshalParam(&msg, val); err != nil {
			fmt.Printf("\tMarshalling: %v\n", val)
			return
		}
	}
	return
}

func marshalParam(buf io.Writer, param interface{}) (err error) {
    v := reflect.ValueOf(param)
	if !v.IsValid() {
		return errors.New("Can not encode value.")
	}

	switch v.Kind() {
	case reflect.Bool:
		x := v.Bool()
		writeByte(buf, vt_BOOL)
		err = writeBoolean(buf, x)
	case reflect.Int8:
		x := v.Int()
		writeByte(buf, vt_BOOL)
		err = writeByte(buf, int8(x))
	case reflect.Int16:
		x := v.Int()
		writeByte(buf, vt_SHORT)
		err = writeShort(buf, int16(x))
	case reflect.Int32:
		x := v.Int()
		writeByte(buf, vt_INT)
		err = writeInt(buf, int32(x))
	case reflect.Int64:
		x := v.Int()
		writeByte(buf, vt_LONG)
		err = writeLong(buf, int64(x))
	case reflect.Float64:
		x := v.Float()
		writeByte(buf, vt_FLOAT)
		err = writeFloat(buf, float64(x))
	case reflect.String:
		x := v.String()
		writeByte(buf, vt_STRING)
		err = writeString(buf, x)
	}
    return
}

// readCallResponse reads a stored procedure invocation response.
func deserializeCallResponse(r io.Reader) (response *Response, err error) {
	response = new(Response)
	if response.clientData, err = readLong(r); err != nil {
		return nil, err
	}

	fields, err := readByte(r)
	if err != nil {
		return nil, err
	} else {
		response.fieldsPresent = uint8(fields)
	}

	if response.status, err = readByte(r); err != nil {
		return nil, err
	}
	if response.fieldsPresent&(1<<5) != 0 {
		if response.statusString, err = readString(r); err != nil {
			return nil, err
		}
	}
	if response.appStatus, err = readByte(r); err != nil {
		return nil, err
	}
	if response.fieldsPresent&(1<<7) != 0 {
		if response.appStatusString, err = readString(r); err != nil {
			return nil, err
		}
	}
	if response.clusterLatency, err = readInt(r); err != nil {
		return nil, err
	}
	if response.fieldsPresent&(1<<6) != 0 {
		if response.exceptionLength, err = readInt(r); err != nil {
			return nil, err
		}
		if response.exceptionLength > 0 {
			fmt.Printf("Received exception of length: %d\n", response.exceptionLength)
			// TODO: implement exception deserialization.
			ignored := make([]byte, response.exceptionLength)
			if _, err = io.ReadFull(r, ignored); err != nil {
				return nil, err
			}
		}
	}
	if response.resultCount, err = readShort(r); err != nil {
		return nil, err
	}
    if kInlineTableDeser {
        response.tables = make([]Table, response.resultCount)
        for idx, _ := range response.tables {
            if response.tables[idx], err = deserializeTable(r); err != nil {
                return nil, err
            }
        }
    } else {
        response.tableBufs = make([]bytes.Buffer, response.resultCount)
        for idx, _ := range response.tableBufs {
            if response.tableBufs[idx], err = readTableBuf(r); err != nil {
                return nil, err
            }
        }
    }
	return response, nil
}

func readTableBuf(r io.Reader) (bytes.Buffer, error) {
    var b bytes.Buffer
    ttlLength, err := readInt(r)
    if err != nil {
        return b, err
    }
    // This copy should be unnecessary; unclear how to create
    // a set of Buffers as views into the data underlying r.
    var data []byte = make([]byte, ttlLength)
    _, err = r.Read(data)
    if err != nil {
        return b, err
    }
    return *bytes.NewBuffer(data), nil
}


//
// Folowing to end of file is dead table deser code from a
// previous arrangement of the api; still useful to doc some
// of the wire protocol, though.
//

func deserializeTable(r io.Reader) (t Table, err error) {
	var errTable Table

	ttlLength, err := readInt(r) // ttlLength
	if err != nil {
		return errTable, err
	}
	metaLength, err := readInt(r) // metaLength
	if err != nil {
		return errTable, err
	}
	fmt.Printf("\tttlLength: %v, metaLength: %v\n", ttlLength, metaLength)

	t.statusCode, err = readByte(r)
	if err != nil {
		return errTable, err
	}

	t.columnCount, err = readShort(r)
	if err != nil {
		return errTable, err
	}

	// column type "array" and column name "array" are not
	// length prefixed arrays. they are really just columnCount
	// len sequences of bytes (col. type) and strings (col names).
	var i int16
	for i = 0; i < t.columnCount; i++ {
		ct, err := readByte(r)
		if err != nil {
			return errTable, err
		}
		t.columnTypes = append(t.columnTypes, ct)
	}

	for i = 0; i < t.columnCount; i++ {
		cn, err := readString(r)
		if err != nil {
			return errTable, err
		}
		t.columnNames = append(t.columnNames, cn)
	}

	t.rowCount, err = readInt(r)
	if err != nil {
		return errTable, err
	}

	t.rows = make([]Row, t.rowCount)
	for idx, _ := range t.rows {
		_, err := readInt(r)
		if err != nil {
			return errTable, err
		}
		row, err := deserializeRow(t.columnTypes, r)
		if err != nil {
			return errTable, err
		}
		t.rows[idx] = row
	}
	return t, nil
}

func deserializeRow(types []int8, r io.Reader) (Row, error) {
	var row Row
	row.vals = make([]interface{}, len(types))
	for idx, t := range types {
		val, err := deserializeType(t, r)
		if err != nil {
			return row, err
		}
		row.vals[idx] = val
	}
	return row, nil
}

func deserializeType(volttype int8, r io.Reader) (interface{}, error) {
	switch volttype {
	case vt_BOOL:
		return readBoolean(r)
	case vt_SHORT:
		return readShort(r)
	case vt_INT:
		return readInt(r)
	case vt_LONG:
		return readLong(r)
	case vt_FLOAT:
		return readFloat(r)
	case vt_STRING:
		return readString(r)
	case vt_TIMESTAMP:
		// TODO: need timestamps...
		return readLong(r)
	case vt_TABLE:
		panic("Can not deserialize table via deserializeType")
	case vt_DECIMAL:
		panic("Decimal type is not supported.")
	case vt_VARBIN:
		panic("VARBINARY type is not supported.")
	default:
		panic("Unknown type in deserialize type")
	}
	panic("Unreachable.")
}

