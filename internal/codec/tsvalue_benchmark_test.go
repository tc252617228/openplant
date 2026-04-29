package codec

import (
	"bytes"
	"testing"

	"github.com/tc252617228/openplant/model"
)

func BenchmarkTSValueR8RoundTrip(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := EncodeTSValue(&buf, model.R8(float64(i))); err != nil {
			b.Fatal(err)
		}
		value, err := DecodeTSValue(&buf, model.TypeR8)
		if err != nil {
			b.Fatal(err)
		}
		if value.Type() != model.TypeR8 {
			b.Fatalf("type=%s", value.Type())
		}
	}
}
