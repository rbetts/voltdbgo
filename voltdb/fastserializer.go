package voltdbgo

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
)

// package private methods that perform voltdb compatible
// serialization. http://community.voltdb.com/docs/WireProtocol/index

const (
	vt_ARRAY     int8 = -99 // array (short)(values*)
	vt_NULL      int8 = 1   // null
	vt_BOOL      int8 = 3   // boolean, byte
	vt_SHORT     int8 = 4   // int16
	vt_INT       int8 = 5   // int32
	vt_LONG      int8 = 6   // int64
	vt_FLOAT     int8 = 8   // float64
	vt_STRING    int8 = 9   // string (int32-length-prefix)(utf-8 bytes)
	vt_TIMESTAMP int8 = 11  // int64 timestamp microseconds
	vt_TABLE     int8 = 21  // VoltTable
	vt_DECIMAL   int8 = 22  // fix-scaled, fix-precision decimal
	vt_VARBIN    int8 = 25  // varbinary (int)(bytes)
)

// SUCCESS = 1
// USER_ABORT = -1
// GRACEFUL_FAILURE = -2
// UNEXPECTED_FAILURE = -3
// CONNECTION_LOST = -4

var order = binary.BigEndian

// protoVersion is the implemented VoltDB wireprotocol version.
const protoVersion = 1

func writeProtoVersion(w io.Writer) error {
	var b [1]byte
	b[0] = protoVersion
	_, err := w.Write(b[:1])
	return err
}

func writeBoolean(w io.Writer, d bool) (err error) {
	if d {
		err = writeByte(w, 0x1)
	} else {
		err = writeByte(w, 0x0)
	}
	return
}

func readBoolean(r io.Reader) (bool, error) {
	val, err := readByte(r)
	if err != nil {
		return false, err
	}
	result := val != 0
	return result, nil
}

func writeByte(w io.Writer, d int8) error {
	var b [1]byte
	b[0] = byte(d)
	_, err := w.Write(b[:1])
	return err
}

func readByte(r io.Reader) (int8, error) {
	var b [1]byte
	bs := b[:1]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

func readByteArray(r io.Reader) ([]int8, error) {
	// byte arrays have 4 byte length prefixes.
	cnt, err := readInt(r)
	if err != nil {
		return nil, err
	}
	arr := make([]int8, cnt)
	for idx := range arr {
		val, err := readByte(r)
		if err != nil {
			return nil, err
		}
		arr[idx] = val
	}
	return arr, nil
}

func writeShort(w io.Writer, d int16) error {
	var b [2]byte
	bs := b[:2]
	order.PutUint16(bs, uint16(d))
	_, err := w.Write(bs)
	return err
}

func readShort(r io.Reader) (int16, error) {
	var b [2]byte
	bs := b[:2]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint16(bs)
	return int16(result), nil
}

func writeInt(w io.Writer, d int32) error {
	var b [4]byte
	bs := b[:4]
	order.PutUint32(bs, uint32(d))
	_, err := w.Write(bs)
	return err
}

func readInt(r io.Reader) (int32, error) {
	var b [4]byte
	bs := b[:4]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint32(bs)
	return int32(result), nil
}

func writeLong(w io.Writer, d int64) error {
	var b [8]byte
	bs := b[:8]
	order.PutUint64(bs, uint64(d))
	_, err := w.Write(bs)
	return err
}

func readLong(r io.Reader) (int64, error) {
	var b [8]byte
	bs := b[:8]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint64(bs)
	return int64(result), nil
}

func writeFloat(w io.Writer, d float64) error {
	var b [8]byte
	bs := b[:8]
	order.PutUint64(bs, uint64(d))
	_, err := w.Write(bs)
	return err
}

func readFloat(r io.Reader) (float64, error) {
	var b [8]byte
	bs := b[:8]
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	result := order.Uint64(bs)
	return float64(result), nil
}

func writeString(w io.Writer, d string) error {
	writeInt(w, int32(len(d)))
	_, err := io.WriteString(w, d)
	return err
}

func readString(r io.Reader) (result string, err error) {
	result = ""
	length, err := readInt(r)
	if err != nil {
		return
	}
	bs := make([]byte, length)
	_, err = r.Read(bs)
	if err != nil {
		return
	}
	return string(bs), nil
}

func readStringArray(r io.Reader) ([]string, error) {
	cnt, err := readShort(r)
	if err != nil {
		return nil, err
	}
	arr := make([]string, cnt)
	for idx := range arr {
		val, err := readString(r)
		if err != nil {
			return nil, err
		}
		arr[idx] = val
	}
	return arr, nil
}

func writeByteString(w io.Writer, d []byte) error {
	writeInt(w, int32(len(d)))
	_, err := w.Write(d)
	return err
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

func serializeCall(proc string, ud int64, params []interface{}) (msg bytes.Buffer, err error) {
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

// readLoginResponse configures conn with server's advertisement.
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

// readCallResponse reads a stored procedure invocation response.
func deserializeResponse(r io.Reader) (response *Response, err error) {
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
	response.tables = make([]Table, response.resultCount)
	fmt.Printf("\tDeserializing %v tables\n", response.resultCount)
	for idx, _ := range response.tables {
		if response.tables[idx], err = deserializeTable(r); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func serializeParams(params []interface{}) (msg bytes.Buffer, err error) {
	// parameter count      short
	// (type byte, parameter)*
	if err = writeShort(&msg, int16(len(params))); err != nil {
		return
	}
	for _, val := range params {
		if err = marshal(&msg, val); err != nil {
			fmt.Printf("\tMarshalling: %v\n", val)
			return
		}
	}
	return
}

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
	fmt.Printf("\trowcount: %v\n", t.rowCount)

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

func marshal(w io.Writer, v interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()
	err = reflectValue(w, reflect.ValueOf(v))
	return err
}

// from encoding.json
func reflectValue(buf io.Writer, v reflect.Value) (err error) {
	if !v.IsValid() {
		return errors.New("Can not encode value.")
	}
	/*
	   // Could implement the Marshal interface
	   // but at the momement, this isn't the approach.
	   m, ok := v.Interface().(Marshaler)
	   if !ok {
	           // T doesn't match the interface. Check against *T too.
	           if v.Kind() != reflect.Ptr && v.CanAddr() {
	                   m, ok = v.Addr().Interface().(Marshaler)
	                   if ok {
	                           v = v.Addr()
	                   }
	           }
	   }
	   if ok && (v.Kind() != reflect.Ptr || !v.IsNil()) {
	           b, err := m.MarshalJSON()
	           if err == nil {
	                   // copy JSON into buffer, checking validity.
	                   err = compact(&e.Buffer, b, true)
	           }
	           if err != nil {
	                   e.error(&MarshalerError{v.Type(), err})
	           }
	           return
	   }
	*/

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
