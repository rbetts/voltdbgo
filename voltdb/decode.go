package voltdbgo

import (
    "bytes"
    "reflect"
)

// Internal methods to unmarshal / reflect a returned table []byte
// into a slice of user provided row structs.

// unmarshal is the decoding entry point.
func unmarshal(data []byte, v interface{}) {
    // TODO: defer a recoverer and panic up the stack to here?

    // must have a pointer to be modifiable
    rv := reflect.ValueOf(v)
    if rv.Kind() != reflect.Ptr || rv.IsNil() {
        panic("Must unmarshal into a pointer to a struct.")
    }

    // must have a struct
    structVal := rv.Elem()
    typeOfT := structVal.Type()
    if typeOfT.Kind() != reflect.Struct {
        panic("Must unmarshal into a struct.")
    }

    // fastserializer wants a buffer.
    buf := bytes.NewBuffer(data)

    // iterate and assign the fields from data
    for i :=0; i < structVal.NumField(); i++ {
        structField := structVal.Field(i)
        switch structField.Type().Kind() {
        case reflect.Int64:
            val, err := readLong(buf)
            if err != nil {
                panic("Error deserializing long in unmarshal")
            }
            structField.SetInt(val)
        }
    }
}



