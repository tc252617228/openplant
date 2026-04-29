package operror

import (
	"errors"
	"fmt"
)

type Kind string

const (
	KindUnknown     Kind = "unknown"
	KindNetwork     Kind = "network"
	KindTimeout     Kind = "timeout"
	KindCanceled    Kind = "canceled"
	KindServer      Kind = "server"
	KindProtocol    Kind = "protocol"
	KindDecode      Kind = "decode"
	KindValidation  Kind = "validation"
	KindUnsafeSQL   Kind = "unsafe_sql"
	KindReadOnly    Kind = "readonly"
	KindClosed      Kind = "closed"
	KindUnsupported Kind = "unsupported"
)

var (
	ErrClosed         = errors.New("openplant: client is closed")
	ErrInvalidOption  = errors.New("openplant: invalid option")
	ErrInvalidQuery   = errors.New("openplant: invalid query")
	ErrUnsafeSQL      = errors.New("openplant: unsafe sql")
	ErrReadOnly       = errors.New("openplant: readonly mode")
	ErrNotImplemented = errors.New("openplant: not implemented")
)

type Error struct {
	Kind    Kind
	Op      string
	Code    int32
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := e.Message
	if msg == "" && e.Err != nil {
		msg = e.Err.Error()
	}
	if e.Op != "" && e.Code != 0 {
		return fmt.Sprintf("%s: %s: code=%d: %s", e.Kind, e.Op, e.Code, msg)
	}
	if e.Op != "" {
		return fmt.Sprintf("%s: %s: %s", e.Kind, e.Op, msg)
	}
	if e.Code != 0 {
		return fmt.Sprintf("%s: code=%d: %s", e.Kind, e.Code, msg)
	}
	return fmt.Sprintf("%s: %s", e.Kind, msg)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func Wrap(kind Kind, op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Op: op, Err: err}
}

func New(kind Kind, op, message string) error {
	return &Error{Kind: kind, Op: op, Message: message}
}

func Server(op string, code int32, message string) error {
	if message == "" {
		message = ServerCodeMessage(code)
	}
	if message == "" {
		message = "OpenPlant server error"
	}
	return &Error{Kind: KindServer, Op: op, Code: code, Message: message}
}

func Validation(op, message string) error {
	return &Error{Kind: KindValidation, Op: op, Message: message, Err: ErrInvalidQuery}
}

func UnsafeSQL(op, message string) error {
	return &Error{Kind: KindUnsafeSQL, Op: op, Message: message, Err: ErrUnsafeSQL}
}

func ReadOnly(op, message string) error {
	return &Error{Kind: KindReadOnly, Op: op, Message: message, Err: ErrReadOnly}
}

func Unsupported(op, message string) error {
	return &Error{Kind: KindUnsupported, Op: op, Message: message, Err: ErrNotImplemented}
}

func IsKind(err error, kind Kind) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind == kind
	}
	switch kind {
	case KindClosed:
		return errors.Is(err, ErrClosed)
	case KindUnsafeSQL:
		return errors.Is(err, ErrUnsafeSQL)
	case KindReadOnly:
		return errors.Is(err, ErrReadOnly)
	case KindValidation:
		return errors.Is(err, ErrInvalidQuery) || errors.Is(err, ErrInvalidOption)
	default:
		return false
	}
}
