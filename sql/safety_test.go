package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/tc252617228/openplant/operror"
)

func TestValidateReadOnlyAllowsExpectedQueries(t *testing.T) {
	queries := []string{
		`select ID,GN from Point where ID in (1,2)`,
		`WITH p AS (SELECT ID FROM Point WHERE ID=1) SELECT * FROM p`,
		`select '-- delete from Point' as text`,
	}
	for _, q := range queries {
		if err := ValidateReadOnly(q); err != nil {
			t.Fatalf("query should be readonly: %q: %v", q, err)
		}
	}
}

func TestValidateReadOnlyRejectsUnsafeQueries(t *testing.T) {
	queries := []string{
		`update Point set ED='x' where ID=1`,
		`select * from Point; select * from Realtime`,
		`select * from Point -- comment`,
		`select * from Point /* comment */`,
		`with x as (select id from point) delete from point where id=1`,
		`insert into Point (GN) values ('W3.A.B')`,
		`show tables`,
		`describe Point`,
		`explain select * from Point where ID=1`,
		`pragma table_info(Point)`,
	}
	for _, q := range queries {
		if err := ValidateReadOnly(q); err == nil {
			t.Fatalf("query should be unsafe: %q", q)
		}
	}
}

func TestExecUnsafeRequiresOptInAndWritableMode(t *testing.T) {
	svc := NewService(Options{ReadOnly: true, AllowUnsafeSQL: true})
	_, err := svc.ExecUnsafe(context.Background(), `update Point set ED='x' where ID=1`)
	if !operror.IsKind(err, operror.KindReadOnly) {
		t.Fatalf("expected readonly error, got %v", err)
	}

	svc = NewService(Options{ReadOnly: false, AllowUnsafeSQL: false})
	_, err = svc.ExecUnsafe(context.Background(), `update Point set ED='x' where ID=1`)
	if !operror.IsKind(err, operror.KindUnsafeSQL) {
		t.Fatalf("expected unsafe sql error, got %v", err)
	}
}

func TestQueryRejectsUnsafeBeforeExecutor(t *testing.T) {
	svc := NewService(Options{})
	_, err := svc.Query(context.Background(), `delete from Point where ID=1`)
	if !operror.IsKind(err, operror.KindUnsafeSQL) {
		t.Fatalf("expected unsafe sql error, got %v", err)
	}
	_, err = svc.Query(context.Background(), `select * from Point where ID=1`)
	if !operror.IsKind(err, operror.KindUnsupported) && !errors.Is(err, operror.ErrNotImplemented) {
		t.Fatalf("expected unsupported pending transport error, got %v", err)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	got, err := QuoteIdentifier("W3.Point")
	if err != nil {
		t.Fatalf("QuoteIdentifier rejected valid identifier: %v", err)
	}
	if got != `"W3"."Point"` {
		t.Fatalf("unexpected quote result %q", got)
	}
	if _, err := QuoteIdentifier(`Point;drop`); err == nil {
		t.Fatalf("expected unsafe identifier to be rejected")
	}
}

func TestQualifiedTable(t *testing.T) {
	got, err := QualifiedTable("W3", "Point")
	if err != nil {
		t.Fatalf("QualifiedTable rejected valid table: %v", err)
	}
	if got != "W3.Point" {
		t.Fatalf("unexpected table result %q", got)
	}
	if _, err := QualifiedTable(`W3";drop`, "Point"); err == nil {
		t.Fatalf("expected unsafe database identifier to be rejected")
	}
	if _, err := QualifiedTable("W3", `Point;drop`); err == nil {
		t.Fatalf("expected unsafe table identifier to be rejected")
	}
}

func TestLikePatternEscaping(t *testing.T) {
	if got := EscapeLikePattern(`A_B%\C`); got != `A\_B\%\\C` {
		t.Fatalf("unexpected escaped LIKE pattern %q", got)
	}
	if got := LiteralLikePrefix(`W3.NODE_`); got != `'W3.NODE\_%' ESCAPE '\'` {
		t.Fatalf("unexpected LIKE prefix literal %q", got)
	}
	if got := LiteralLikeContains(`一期01#_`); got != `'%一期01#\_%' ESCAPE '\'` {
		t.Fatalf("unexpected LIKE contains literal %q", got)
	}
}
