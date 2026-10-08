package sqlbackend

import (
	"strings"
	"testing"
)

func FuzzSplitCompoundQuery(f *testing.F) {
	f.Add("")
	f.Add("select 1;select 2")
	f.Add(`select "a;b";select "c\";d"`)
	f.Add(";;;")

	f.Fuzz(func(t *testing.T, query string) {
		backend := NewSimpleSQLBackend(nil)
		parts, err := backend.SplitCompoundQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(parts, ";"); got != query {
			t.Fatalf("joining split query changed it: got %q, want %q", got, query)
		}
	})
}
