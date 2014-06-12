package cube

import (
	"bytes"
	"fmt"
)

// A Palo cube coordinates.
type Coord []int

// Returns csv version of the coordinates.
func (c Coord) String() string {
	b := bytes.NewBuffer(nil)
	for i, n := range c {
		fmt.Fprintf(b, "%d", n)
		if i < len(c)-1 {
			b.WriteRune(',')
		}
	}
	return b.String()
}

// An area of coordinates which can specify multiple values for any dimension.
type CoordArea map[int][]int

// Trasform the area in coordinates.
func (c CoordArea) Split(dims []int) ([]Coord, error) {
	var l = 1
	for _, values := range c {
		l = l * len(values)
	}
	var coords = make([]Coord, l)
	var i = 0

	l = 1
	for _, dimId := range dims {
		values, ok := c[dimId]
		if !ok {
			return nil, fmt.Errorf("dimension %d not found", dimId)
		}
		l = l * len(values)
		for i = 0; i < len(coords); i++ {
			if coords[i] == nil {
				coords[i] = Coord{}
			}
			var a = (i * l / len(coords)) % len(values)
			coords[i] = append(coords[i], values[a])
		}
	}
	return coords, nil
}

type CoordErr struct {
	// A map of error for each coordinate.
	ErrorMap map[string]error
}

func (e *CoordErr) Error() string {
	b := bytes.NewBufferString("coord errors occured:")
	for dim, err := range e.ErrorMap {
		b.WriteString(fmt.Sprintf("\n[%s] %s", dim, err))
	}
	return b.String()
}
