package calc

import "testing"

func TestLookupFunctionNormalizesOPPrefix(t *testing.T) {
	fn, ok := LookupFunction(" value ")
	if !ok {
		t.Fatalf("value was not found")
	}
	if fn.Name != "op.value" || fn.Category != CategoryPointSnapshot {
		t.Fatalf("unexpected function: %#v", fn)
	}
}

func TestLookupFunctionPreservesStatusMethods(t *testing.T) {
	fn, ok := LookupFunction("good")
	if !ok {
		t.Fatalf("good was not found")
	}
	if fn.Name != "good" || fn.Category != CategoryStatusMethod {
		t.Fatalf("unexpected function: %#v", fn)
	}
}

func TestStatisticCatalogMarksStdevNotImplemented(t *testing.T) {
	fn, ok := LookupFunction("op.stdev")
	if !ok {
		t.Fatalf("op.stdev was not found")
	}
	if fn.Implemented {
		t.Fatalf("op.stdev should be cataloged as not implemented")
	}
}

func TestNamesByCategory(t *testing.T) {
	names := NamesByCategory(CategoryMirror)
	if len(names) != 2 || names[0] != "op.ar_sync_time" || names[1] != "op.rt_sync_time" {
		t.Fatalf("mirror names=%#v", names)
	}
}

func TestFunctionsReturnsCopy(t *testing.T) {
	items := Functions()
	items[0].Name = "changed"
	if again := Functions(); again[0].Name == "changed" {
		t.Fatalf("Functions returned mutable backing slice")
	}
}
