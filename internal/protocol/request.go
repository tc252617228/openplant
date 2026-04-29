package protocol

import (
	"bytes"
	"fmt"
	"io"

	"github.com/tc252617228/openplant/internal/codec"
	"github.com/tc252617228/openplant/operror"
)

type Request struct {
	Props map[string]any
	Body  []byte
}

func NewRequest(action, table string) Request {
	return Request{Props: map[string]any{
		PropService: "openplant",
		PropAction:  action,
		PropTable:   table,
	}}
}

func (r Request) Encode() ([]byte, error) {
	var buf bytes.Buffer
	if err := codec.NewEncoder(&buf).EncodeMap(r.Props); err != nil {
		return nil, operror.Wrap(operror.KindProtocol, "protocol.Request.Encode", err)
	}
	if len(r.Body) > 0 {
		if _, err := buf.Write(r.Body); err != nil {
			return nil, err
		}
	}
	if err := codec.NewEncoder(&buf).EncodeValue(nil); err != nil {
		return nil, operror.Wrap(operror.KindProtocol, "protocol.Request.Encode", err)
	}
	return buf.Bytes(), nil
}

type Response struct {
	Props map[string]any
	Body  []byte
}

func DecodeResponse(data []byte) (Response, error) {
	reader := bytes.NewReader(data)
	value, err := codec.NewDecoder(reader).DecodeValue()
	if err != nil {
		return Response{}, operror.Wrap(operror.KindDecode, "protocol.DecodeResponse", err)
	}
	props, ok := value.(map[string]any)
	if !ok {
		return Response{}, operror.New(operror.KindProtocol, "protocol.DecodeResponse", fmt.Sprintf("response props are %T", value))
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return Response{}, operror.Wrap(operror.KindDecode, "protocol.DecodeResponse", err)
	}
	resp := Response{Props: props, Body: body}
	if err := resp.ServerError(); err != nil {
		return resp, err
	}
	return resp, nil
}

func (r Response) Columns() ([]codec.Column, error) {
	return codec.DecodeColumns(r.Props[PropColumns])
}

func (r Response) Rows() ([]map[string]any, error) {
	columns, err := r.Columns()
	if err != nil {
		return nil, err
	}
	return codec.DecodeDataSet(r.Body, columns)
}

func (r Response) ServerError() error {
	code := int32Value(r.Props[PropErrNo])
	if code == 0 {
		return nil
	}
	msg, _ := r.Props[PropError].(string)
	return operror.Server("protocol.Response.ServerError", code, msg)
}

func int32Value(v any) int32 {
	switch x := v.(type) {
	case int8:
		return int32(x)
	case uint8:
		return int32(x)
	case int16:
		return int32(x)
	case uint16:
		return int32(x)
	case int32:
		return x
	case uint32:
		return int32(x)
	case int64:
		return int32(x)
	case uint64:
		return int32(x)
	case int:
		return int32(x)
	case uint:
		return int32(x)
	default:
		return 0
	}
}
