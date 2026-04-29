package calc

import (
	"strings"
	"unicode"

	"github.com/tc252617228/openplant/model"
)

type FormulaIssueKind string

const (
	FormulaIssueEmpty              FormulaIssueKind = "empty"
	FormulaIssueUnterminatedString FormulaIssueKind = "unterminated_string"
	FormulaIssueUnterminatedBlock  FormulaIssueKind = "unterminated_block"
	FormulaIssueReservedOP         FormulaIssueKind = "reserved_op"
	FormulaIssueUnknownFunction    FormulaIssueKind = "unknown_function"
)

type FormulaIssue struct {
	Kind     FormulaIssueKind
	Message  string
	Token    string
	Position int
}

func FormulaReferences(formula string) []model.GN {
	literals, _ := luaStringLiterals(formula)
	seen := make(map[model.GN]struct{}, len(literals))
	out := make([]model.GN, 0, len(literals))
	for _, literal := range literals {
		value := strings.TrimSpace(literal.value)
		if !looksLikeGN(value) {
			continue
		}
		gn := model.GN(value)
		if _, ok := seen[gn]; ok {
			continue
		}
		seen[gn] = struct{}{}
		out = append(out, gn)
	}
	return out
}

func UsesFunction(formula, name string) bool {
	target, ok := LookupFunction(name)
	if !ok {
		return false
	}
	code, _ := maskLuaNonCode(formula)
	for _, call := range opFunctionCalls(code) {
		if call.name == target.Name {
			return true
		}
	}
	return false
}

func LintFormula(formula string) []FormulaIssue {
	issues := make([]FormulaIssue, 0)
	if strings.TrimSpace(formula) == "" {
		issues = append(issues, FormulaIssue{
			Kind:     FormulaIssueEmpty,
			Message:  "formula is empty",
			Position: 0,
		})
		return issues
	}
	_, literalIssues := luaStringLiterals(formula)
	issues = append(issues, literalIssues...)
	code, maskIssues := maskLuaNonCode(formula)
	issues = append(issues, maskIssues...)
	if pos, ok := opAssignmentPosition(code); ok {
		issues = append(issues, FormulaIssue{
			Kind:     FormulaIssueReservedOP,
			Message:  "formula must not redefine the OpenPlant op object",
			Token:    "op",
			Position: pos,
		})
	}
	for _, call := range opFunctionCalls(code) {
		if _, ok := LookupFunction(call.name); !ok {
			issues = append(issues, FormulaIssue{
				Kind:     FormulaIssueUnknownFunction,
				Message:  "unknown OpenPlant calculation function",
				Token:    call.name,
				Position: call.position,
			})
		}
	}
	return issues
}

type luaLiteral struct {
	value    string
	position int
}

func luaStringLiterals(formula string) ([]luaLiteral, []FormulaIssue) {
	literals := make([]luaLiteral, 0)
	issues := make([]FormulaIssue, 0)
	for i := 0; i < len(formula); {
		if isLineCommentStart(formula, i) {
			i = skipLuaComment(formula, i)
			continue
		}
		quote := formula[i]
		if quote != '\'' && quote != '"' {
			i++
			continue
		}
		start := i
		i++
		var b strings.Builder
		closed := false
		for i < len(formula) {
			ch := formula[i]
			if ch == '\\' {
				if i+1 >= len(formula) {
					i++
					continue
				}
				b.WriteByte(formula[i+1])
				i += 2
				continue
			}
			if ch == quote {
				i++
				closed = true
				break
			}
			b.WriteByte(ch)
			i++
		}
		if !closed {
			issues = append(issues, FormulaIssue{
				Kind:     FormulaIssueUnterminatedString,
				Message:  "string literal is not terminated",
				Token:    string(quote),
				Position: start,
			})
			break
		}
		literals = append(literals, luaLiteral{value: b.String(), position: start})
	}
	return literals, issues
}

func maskLuaNonCode(formula string) (string, []FormulaIssue) {
	var b strings.Builder
	b.Grow(len(formula))
	issues := make([]FormulaIssue, 0)
	for i := 0; i < len(formula); {
		if isLineCommentStart(formula, i) {
			end, closed := commentEnd(formula, i)
			if !closed {
				issues = append(issues, FormulaIssue{
					Kind:     FormulaIssueUnterminatedBlock,
					Message:  "block comment is not terminated",
					Token:    "--[[",
					Position: i,
				})
			}
			writeSpaces(&b, end-i)
			i = end
			continue
		}
		quote := formula[i]
		if quote != '\'' && quote != '"' {
			b.WriteByte(formula[i])
			i++
			continue
		}
		start := i
		i++
		for i < len(formula) {
			ch := formula[i]
			if ch == '\\' {
				if i+1 >= len(formula) {
					i++
				} else {
					i += 2
				}
				continue
			}
			i++
			if ch == quote {
				break
			}
		}
		writeSpaces(&b, i-start)
	}
	return b.String(), issues
}

type opCall struct {
	name     string
	position int
}

func opFunctionCalls(code string) []opCall {
	calls := make([]opCall, 0)
	for i := 0; i+3 <= len(code); i++ {
		if code[i:i+3] != "op." {
			continue
		}
		if i > 0 && (isIdentByte(code[i-1]) || code[i-1] == '.') {
			continue
		}
		nameStart := i + 3
		nameEnd := nameStart
		for nameEnd < len(code) && isIdentByte(code[nameEnd]) {
			nameEnd++
		}
		if nameEnd == nameStart {
			continue
		}
		calls = append(calls, opCall{
			name:     "op." + code[nameStart:nameEnd],
			position: i,
		})
		i = nameEnd - 1
	}
	return calls
}

func opAssignmentPosition(code string) (int, bool) {
	for i := 0; i < len(code); i++ {
		if i+2 > len(code) || code[i:i+2] != "op" {
			continue
		}
		if i > 0 && isIdentByte(code[i-1]) {
			continue
		}
		j := i + 2
		if j < len(code) && (isIdentByte(code[j]) || code[j] == '.') {
			continue
		}
		for j < len(code) && isSpaceByte(code[j]) {
			j++
		}
		if j < len(code) && code[j] == '=' {
			return i, true
		}
	}
	return 0, false
}

func looksLikeGN(value string) bool {
	if value == "" || strings.ContainsAny(value, " \t\r\n") {
		return false
	}
	if strings.Count(value, ".") < 2 {
		return false
	}
	if allDigitsAndDots(value) {
		return false
	}
	gn := model.GN(value)
	return gn.Validate() == nil
}

func allDigitsAndDots(value string) bool {
	for _, r := range value {
		if r != '.' && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func isLineCommentStart(s string, i int) bool {
	return i+1 < len(s) && s[i] == '-' && s[i+1] == '-'
}

func skipLuaComment(s string, i int) int {
	end, _ := commentEnd(s, i)
	return end
}

func commentEnd(s string, i int) (int, bool) {
	if i+3 < len(s) && s[i:i+4] == "--[[" {
		end := strings.Index(s[i+4:], "]]")
		if end < 0 {
			return len(s), false
		}
		return i + 4 + end + 2, true
	}
	for i < len(s) && s[i] != '\n' {
		i++
	}
	return i, true
}

func writeSpaces(b *strings.Builder, count int) {
	for i := 0; i < count; i++ {
		b.WriteByte(' ')
	}
}

func isIdentByte(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

func isSpaceByte(ch byte) bool {
	return unicode.IsSpace(rune(ch))
}
