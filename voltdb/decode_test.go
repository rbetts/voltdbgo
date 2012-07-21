package voltdbgo

import (
    "bytes"
    "testing"
)

func TestBasicUnmarshal(t *testing.T) {
    var b bytes.Buffer
    writeLong(&b, 57890)

    type T1 struct {
        V1 int64
    }

    t1 := &T1{}
    unmarshal(b.Bytes(), t1)
    if t1.V1 != 57890 {
        t.Errorf("Failed to unmarshal T1.V1")
    }
}


