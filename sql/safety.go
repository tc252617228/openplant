package sql

import (
	"fmt"
	"strings"

	"github.com/tc252617228/openplant/operror"
)

func ValidateReadOnly(query string) error {
	clean, err := cleanSQL(query)
	if err != nil {
		return err
	}
	if clean == "" {
		return operror.UnsafeSQL("sql.ValidateReadOnly", "SQL is empty")
	}
	if containsAnyWord(clean, "insert", "update", "delete", "drop", "alter", "truncate", "create", "grant", "revoke", "replace", "merge", "call", "exec", "attach", "detach") {
		return operror.UnsafeSQL("sql.ValidateReadOnly", "SQL contains a mutation or administrative keyword")
	}
	switch firstKeyword(clean, 0) {
	case "select":
		return nil
	case "with":
		if isReadOnlyWith(clean) {
			return nil
		}
		return operror.UnsafeSQL("sql.ValidateReadOnly", "WITH query does not end in a readonly statement")
	default:
		return operror.UnsafeSQL("sql.ValidateReadOnly", "SQL must start with a readonly keyword")
	}
}

func IsReadOnly(query string) bool {
	return ValidateReadOnly(query) == nil
}

func cleanSQL(query string) (string, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return "", nil
	}
	var b strings.Builder
	inSingle := false
	inDouble := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(trimmed) && trimmed[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '"' {
				if i+1 < len(trimmed) && trimmed[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			b.WriteByte(' ')
		case '"':
			inDouble = true
			b.WriteByte(' ')
		case ';':
			return "", operror.UnsafeSQL("sql.ValidateReadOnly", "semicolon is not allowed")
		case '-':
			if i+1 < len(trimmed) && trimmed[i+1] == '-' {
				return "", operror.UnsafeSQL("sql.ValidateReadOnly", "line comments are not allowed")
			}
			b.WriteByte(ch)
		case '/':
			if i+1 < len(trimmed) && trimmed[i+1] == '*' {
				return "", operror.UnsafeSQL("sql.ValidateReadOnly", "block comments are not allowed")
			}
			b.WriteByte(ch)
		default:
			b.WriteByte(ch)
		}
	}
	if inSingle || inDouble {
		return "", operror.UnsafeSQL("sql.ValidateReadOnly", "unterminated string literal")
	}
	return strings.TrimSpace(strings.ToLower(b.String())), nil
}

func isReadOnlyWith(query string) bool {
	idx := nextToken(query, 0)
	kw, next := readKeyword(query, idx)
	if kw != "with" {
		return false
	}
	idx = nextToken(query, next)
	if kw, next = readKeyword(query, idx); kw == "recursive" {
		idx = nextToken(query, next)
	}
	for {
		_, next = readKeyword(query, idx)
		if next == idx {
			return false
		}
		idx = nextToken(query, next)
		if idx < len(query) && query[idx] == '(' {
			end, ok := consumeBalanced(query, idx)
			if !ok {
				return false
			}
			idx = nextToken(query, end)
		}
		kw, next = readKeyword(query, idx)
		if kw != "as" {
			return false
		}
		idx = nextToken(query, next)
		if idx >= len(query) || query[idx] != '(' {
			return false
		}
		end, ok := consumeBalanced(query, idx)
		if !ok {
			return false
		}
		idx = nextToken(query, end)
		if idx >= len(query) || query[idx] != ',' {
			break
		}
		idx = nextToken(query, idx+1)
	}
	switch firstKeyword(query, idx) {
	case "select":
		return true
	default:
		return false
	}
}

func firstKeyword(query string, start int) string {
	kw, _ := readKeyword(query, nextToken(query, start))
	return kw
}

func readKeyword(query string, start int) (string, int) {
	i := nextToken(query, start)
	begin := i
	for i < len(query) && isKeywordChar(query[i]) {
		i++
	}
	if begin == i {
		return "", begin
	}
	return query[begin:i], i
}

func nextToken(query string, start int) int {
	for start < len(query) {
		switch query[start] {
		case ' ', '\t', '\n', '\r':
			start++
		default:
			return start
		}
	}
	return start
}

func isKeywordChar(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_'
}

func consumeBalanced(query string, start int) (int, bool) {
	if start >= len(query) || query[start] != '(' {
		return start, false
	}
	depth := 0
	for i := start; i < len(query); i++ {
		switch query[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1, true
			}
			if depth < 0 {
				return i, false
			}
		}
	}
	return len(query), false
}

func containsAnyWord(s string, words ...string) bool {
	for _, word := range words {
		if containsWord(s, word) {
			return true
		}
	}
	return false
}

func containsWord(s, word string) bool {
	if word == "" {
		return false
	}
	for idx := 0; ; {
		pos := strings.Index(s[idx:], word)
		if pos == -1 {
			return false
		}
		pos += idx
		after := pos + len(word)
		if (pos == 0 || !isWordChar(s[pos-1])) && (after >= len(s) || !isWordChar(s[after])) {
			return true
		}
		idx = after
	}
}

func isWordChar(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_'
}

func QuoteIdentifier(identifier string) (string, error) {
	if identifier == "" {
		return "", fmt.Errorf("identifier is required")
	}
	parts := strings.Split(identifier, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		if !validIdentifierPart(part) {
			return "", fmt.Errorf("invalid identifier %q", identifier)
		}
		quoted = append(quoted, `"`+part+`"`)
	}
	return strings.Join(quoted, "."), nil
}

func QualifiedTable(database, table string) (string, error) {
	if !validIdentifierPart(database) {
		return "", fmt.Errorf("invalid database identifier %q", database)
	}
	if !validIdentifierPart(table) {
		return "", fmt.Errorf("invalid table identifier %q", table)
	}
	return database + "." + table, nil
}

func validIdentifierPart(part string) bool {
	if part == "" {
		return false
	}
	for i, ch := range part {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch == '_':
		case i > 0 && ch >= '0' && ch <= '9':
		default:
			return false
		}
	}
	return true
}

func LiteralString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

const likeEscapeClause = ` ESCAPE '\'`

// EscapeLikePattern escapes SQL LIKE wildcard characters in a user fragment.
func EscapeLikePattern(v string) string {
	var b strings.Builder
	for _, r := range v {
		switch r {
		case '\\', '%', '_':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// LiteralLikePattern renders a LIKE pattern literal plus OpenPlant's ESCAPE clause.
// The pattern may already contain intentional % wildcards.
func LiteralLikePattern(pattern string) string {
	return LiteralString(pattern) + likeEscapeClause
}

// LiteralLikePrefix renders a prefix match where the prefix itself is literal text.
func LiteralLikePrefix(prefix string) string {
	return LiteralLikePattern(EscapeLikePattern(prefix) + "%")
}

// LiteralLikeContains renders a contains match where the value itself is literal text.
func LiteralLikeContains(value string) string {
	return LiteralLikePattern("%" + EscapeLikePattern(value) + "%")
}
