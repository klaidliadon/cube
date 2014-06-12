package cube

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
)

type fixable interface {
	fixme()
}

func internalErr(msg string) *PaloError {
	return &PaloError{Message: msg}
}

// Creates a new resultRow object from a string.
func newResultRow(s string) (row resultRow, err error) {
	r := bufio.NewReader(bytes.NewBufferString(s))
	for err == nil {
		var b byte
		if p, err := r.Peek(1); err != nil {
			break
		} else {
			b = p[0]
		}
		end := _semicolon
		if b == _quote {
			r.ReadByte()
			end = _quote
		}
		v, err := r.ReadString(byte(end))
		if err != nil {
			break
		}
		if b == '"' {
			b, err = r.ReadByte()
			if err != nil {
				break
			}
			if b != byte(';') {
				return nil, fmt.Errorf("unexpected %q", rune(b))
			}
		}
		row = append(row, resultField(strings.Trim(v, `;"`)))
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	return row, nil
}

// A row of the palo response.
type resultRow []resultField

// Checks the existence of a field in the row.
func (p resultRow) HasField(n int) bool {
	return len(p) > n
}

func canUnmarshal(f reflect.Type) bool {
	switch f.Kind() {
	case reflect.Int, reflect.Int64, reflect.String, reflect.Float32, reflect.Float64:
		return true
	case reflect.Slice:
		return canUnmarshal(f.Elem())
	}
	return false
}

// Populate a struct with strings, integers and slices of strings or integers.
// The struct must have a field named `Data` (another struct). Example:
// 		type Mystruct struct {
//			Data struct {Id int, Name string}
//		}
func (p resultRow) Unmarshal(v interface{}) error {
	t := reflect.TypeOf(v)
	r := reflect.ValueOf(v)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return errors.New("struct pointer needed")
	}
	finalValue := reflect.ValueOf(nil)
	if r.IsNil() {
		finalValue = reflect.New(r.Type().Elem()).Elem()
	} else {
		finalValue = r.Elem()
	}
	nr := finalValue.FieldByName("Data")
	nt := nr.Type()
	for i, j := 0, 0; i < nt.NumField(); i++ {
		f := nt.Field(i)
		fv := nr.Field(i)
		if !fv.CanSet() {
			continue
		}
		if !canUnmarshal(f.Type) {
			continue
		}
		if !p.HasField(j) {
			break
		}
		if f.Type.Kind() == reflect.Slice {
			var v = reflect.MakeSlice(reflect.SliceOf(f.Type.Elem()), 0, len(p[j].Array()))
			et := f.Type.Elem()
			for _, s := range p[j].Array() {
				a, err := getValue(et, s)
				if err != nil {
					return fmt.Errorf("field %s: %s", f.Name, err)
				}
				v = reflect.Append(v, reflect.ValueOf(a))
			}
			fv.Set(v)
		} else {
			value, err := getValue(f.Type, p[j].String())
			if err != nil {
				return err
			}
			fv.Set(reflect.ValueOf(value))
		}
		j++
	}
	r.Elem().Set(finalValue)
	if vi, ok := v.(fixable); ok {
		vi.fixme()
	}
	return nil
}

func getValue(t reflect.Type, s string) (interface{}, error) {
	switch k := t.Kind(); k {
	case reflect.String:
		return s, nil
	case reflect.Int, reflect.Int32, reflect.Int64:
		if s == "" {
			s = "0"
		}
		intV, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		switch k {
		case reflect.Int:
			return int(intV), nil
		case reflect.Int32:
			return int32(intV), nil
		case reflect.Int64:
			return int64(intV), nil
		}
		return int(intV), nil
	case reflect.Float32, reflect.Float64:
		if s == "" {
			s = "0"
		}
		floatV, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		switch k {
		case reflect.Float32:
			return float32(floatV), nil
		case reflect.Float64:
			return float64(floatV), nil
		}
	}
	return nil, nil
}

// A field in a resultRow.
type resultField string

// Return the field value as a string.
func (f resultField) String() string {
	return string(f)
}

// Return the field value as a slice of strings.
func (f resultField) Array() []string {
	if string(f) == "" {
		return nil
	}
	return strings.Split(string(f), ",")
}

const _quote = '"'
const _semicolon = ';'
