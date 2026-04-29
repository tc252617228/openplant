package calc

import (
	"testing"

	"github.com/tc252617228/openplant/model"
)

func TestFormulaReferencesExtractsUniquePointGNs(t *testing.T) {
	got := FormulaReferences(`return op.value("W3.SYS.DBMEM") / op.value('W3.SYS.MEMTOTAL') + op.ping("192.168.2.239") + op.cacheq("W3") + op.value("W3.SYS.DBMEM")`)
	want := []model.GN{"W3.SYS.DBMEM", "W3.SYS.MEMTOTAL"}
	if len(got) != len(want) {
		t.Fatalf("refs=%#v want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("refs=%#v want %#v", got, want)
		}
	}
}

func TestLintFormulaDetectsReservedOPAssignment(t *testing.T) {
	issues := LintFormula(`local op = {}; return op.value("W3.N.P1")`)
	if !hasIssue(issues, FormulaIssueReservedOP) {
		t.Fatalf("expected reserved op issue: %#v", issues)
	}
}

func TestLintFormulaIgnoresStringsAndComments(t *testing.T) {
	issues := LintFormula(`-- op.nope()
return "op.nope()" .. op.value("W3.N.P1")`)
	if len(issues) != 0 {
		t.Fatalf("unexpected issues: %#v", issues)
	}
}

func TestLintFormulaDetectsUnknownFunction(t *testing.T) {
	issues := LintFormula(`return op.unknown("W3.N.P1")`)
	if !hasIssue(issues, FormulaIssueUnknownFunction) {
		t.Fatalf("expected unknown function issue: %#v", issues)
	}
}

func TestLintFormulaDetectsUnterminatedStringAndBlockComment(t *testing.T) {
	issues := LintFormula(`--[[ comment`)
	if !hasIssue(issues, FormulaIssueUnterminatedBlock) {
		t.Fatalf("expected unterminated block issue: %#v", issues)
	}
	issues = LintFormula(`return op.value("W3.N.P1)`)
	if !hasIssue(issues, FormulaIssueUnterminatedString) {
		t.Fatalf("expected unterminated string issue: %#v", issues)
	}
}

func TestUsesFunction(t *testing.T) {
	if !UsesFunction(`return op.rate("W3.SYS.EVENT", 5)`, "rate") {
		t.Fatalf("expected rate usage")
	}
	if UsesFunction(`return op.value("W3.N.P1")`, "op.rate") {
		t.Fatalf("did not expect rate usage")
	}
}

func hasIssue(issues []FormulaIssue, kind FormulaIssueKind) bool {
	for _, issue := range issues {
		if issue.Kind == kind {
			return true
		}
	}
	return false
}
