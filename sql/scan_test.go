package sql

import (
	"testing"
	"time"
)

func TestScanRowsUsesTagsAndConversions(t *testing.T) {
	type sampleRow struct {
		ID      int32     `openplant:"ID"`
		GN      string    `openplant:"GN"`
		Time    time.Time `openplant:"TM"`
		Status  int16     `openplant:"DS"`
		Enabled bool      `openplant:"EN"`
		Blob    []byte    `openplant:"BN"`
	}

	got, err := ScanRows[sampleRow]([]Row{{
		"ID": int64(1001),
		"GN": []byte("W3.N.P1"),
		"TM": int32(123456),
		"DS": int32(3),
		"EN": int8(1),
		"BN": []byte{1, 2, 3},
	}})
	if err != nil {
		t.Fatalf("ScanRows failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("rows=%d want 1", len(got))
	}
	row := got[0]
	if row.ID != 1001 || row.GN != "W3.N.P1" || row.Status != 3 || !row.Enabled {
		t.Fatalf("unexpected scan result: %#v", row)
	}
	if !row.Time.Equal(time.Unix(123456, 0)) {
		t.Fatalf("time=%s", row.Time)
	}
	if len(row.Blob) != 3 || row.Blob[0] != 1 || row.Blob[2] != 3 {
		t.Fatalf("blob=%#v", row.Blob)
	}
}

func TestScanRowUsesUppercaseFieldMatch(t *testing.T) {
	type row struct {
		ID int32
	}
	got, err := ScanRow[row](Row{"ID": int32(7)})
	if err != nil {
		t.Fatalf("ScanRow failed: %v", err)
	}
	if got.ID != 7 {
		t.Fatalf("ID=%d want 7", got.ID)
	}
}

func TestScanRowRejectsNonStruct(t *testing.T) {
	if _, err := ScanRow[int](Row{"ID": int32(7)}); err == nil {
		t.Fatalf("expected non-struct target to fail")
	}
}
