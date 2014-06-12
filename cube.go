package cube

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// date role : format - suffix
var roleMap = map[string]func(time.Time) string{
	"year":    func(t time.Time) string { return strconv.Itoa(t.Year()) },
	"isoyear": func(t time.Time) string { y, _ := t.ISOWeek(); return strconv.Itoa(y) },
	"month":   func(t time.Time) string { return fmt.Sprintf("%02d", int(t.Month())) },
	"isoweek": func(t time.Time) string { _, iw := t.ISOWeek(); return fmt.Sprintf("%02d", iw) },
	"weekday": func(t time.Time) string { return strconv.Itoa(int((t.Weekday()+6)%7 + 1)) }, // 1...7
	"day":     func(t time.Time) string { return fmt.Sprintf("%02d", t.Day()) },
}

// Return a new cube using the given name and configuration.
func New(name string, c Config) (*Cube, error) {
	client, err := newClient(c)
	if err != nil {
		return nil, err
	}
	cb, err := client.GetCube(name, false)
	if err != nil {
		return nil, err
	}
	err = cb.init()
	if err != nil {
		return nil, err
	}
	return cb, nil
}

// An OLAP Cube
type Cube struct {
	client      *client
	hash        string
	tags        map[string]string
	dims        cache
	group       map[string][]*Dim
	isAttribute bool
	Data        struct {
		Id           int    //Identifier of the cube
		Name         string //Name of the cube
		DimNumber    int    //Number of dimensions
		Dimensions   []int  //Comma separate list of dimension identifiers;
		NumCells     int64  //Total number of cells
		NumFillCells int64  //Number of filled numeric base cells plus number of filled string cells
		Status       int    //Status of cube (0=unloaded, 1=loaded and 2=changed)
		Type         int    //Type of cube (0=normal, 1=system, 2=attribute, 3=user info, 4=gpu type)
		CubeToken    int    //The cube token of the cube
	}
}

func (c *Cube) doRequest(url string, p params) (result []resultRow, pe *PaloError) {
	p.Add("cube", fmt.Sprintf("%v", c.Data.Id))
	return c.client.doRequest(url, p)
}

func (c *Cube) init() error {
	err := c.initDims()
	if err != nil {
		return err
	}
	return nil
}

func (c *Cube) initDims() error {
	c.dims = newCache()
	c.group = make(map[string][]*Dim)
	p := params{}
	if c.isAttribute {
		p.Add("show_attribute", "1")
	}
	rows, err := c.doRequest("/database/dimensions", p)
	if err != nil {
		return fmt.Errorf("dims init: %s", err)
	}
	for i := 0; i < len(rows); i++ {
		var dm Dim
		err := rows[i].Unmarshal(&dm)
		if err != nil {
			return fmt.Errorf("dims init: bad row %d (%s)", i, err)
		}
		if g := dm.tags["group"]; g != "" {
			c.group[g] = append(c.group[g], &dm)
		}
		for _, dimId := range c.Data.Dimensions {
			if dimId == dm.Data.Id {
				dm.cube = c
				c.dims.Add(&dm)
				continue
			}
		}
	}
	return nil
}

func (c *Cube) canCoord(t reflect.Type) bool {
	if t.Kind() == reflect.Slice {
		return c.canCoord(t.Elem())
	}
	return t.Kind() == reflect.String || t.String() == "time.Time"
}

// Trasform the given object in coordinates.
func (c *Cube) Coords(v interface{}) ([]Coord, error) {
	if m, ok := v.(map[string]string); ok {
		coord, err := c.coordMap(m)
		if err != nil {
			return nil, err
		}
		return []Coord{coord}, nil
	}
	if m, ok := v.(map[string][]string); ok {
		return c.coordsMap(m)
	}
	t := reflect.TypeOf(v)
	r := reflect.ValueOf(v)
	if t.Kind() != reflect.Struct {
		return nil, errors.New("struct, map[string]string or map[string][]string needed")
	}
	var m = make(map[string][]string)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		fv := r.Field(i)
		if !c.canCoord(f.Type) {
			continue
		}
		key := f.Name
		if tag := f.Tag.Get("palo"); tag != "" {
			key = tag
		}
		if f.Type.Kind() == reflect.Slice {
			for i := 0; i < fv.Len(); i++ {
				c.trasform(f.Type.Elem(), fv.Index(i), m, key)
			}
		} else {
			c.trasform(f.Type, fv, m, key)
		}
	}
	return c.coordsMap(m)
}

func (c *Cube) trasform(t reflect.Type, v reflect.Value, m map[string][]string, key string) {
	switch {
	case t.Kind() == reflect.String:
		m[key] = append(m[key], v.String())
	case t.String() == "time.Time":
		for _, dm := range c.group[key] {
			r := dm.tags["role"]
			m[dm.Name()] = append(m[dm.Name()], roleMap[r](v.Interface().(time.Time)))
		}
	}
}

func (c *Cube) coordMap(smap map[string]string) (Coord, error) {
	coord := Coord{}
	for _, dimId := range c.Data.Dimensions {
		dm, err := c.dim(dimId)
		if err != nil {
			return nil, fmt.Errorf("dimension %d missing: %s", dimId, err)
		}
		elName, ok := smap[dm.Name()]
		if !ok {
			return nil, fmt.Errorf("dimension %s missing", dm.Name())
		}
		el, err := dm.Elem(elName)
		if err != nil {
			return nil, err
		}
		coord = append(coord, el.Id())
	}
	return coord, nil
}

func (c *Cube) coordsMap(smap map[string][]string) ([]Coord, error) {
	intMap := CoordArea{}

	for _, dimId := range c.Data.Dimensions {
		dm, err := c.dim(dimId)
		if err != nil {
			return nil, fmt.Errorf("dimension %d missing: %s", dimId, err)
		}
		v, ok := smap[dm.Name()]
		if !ok {
			return nil, fmt.Errorf("dimension %s missing", dm.Name())
		}
		var arr []int
		for _, n := range v {
			el, err := dm.Elem(n)
			if err != nil {
				return nil, err
			}
			arr = append(arr, el.Id())
		}
		intMap[dm.Id()] = arr
	}
	return intMap.Split(c.Data.Dimensions)
}

// Return the list of dimension names.
func (c *Cube) DimNames() ([]string, error) {
	if c.dims.Empty() {
		err := c.init()
		if err != nil {
			return nil, fmt.Errorf("cannot get dim names")
		}
	}
	return c.dims.Names(), nil
}

// Return a dimension by its name.
func (c *Cube) Dim(name string) (*Dim, error) {
	if c.dims.Empty() {
		err := c.init()
		if err != nil {
			return nil, fmt.Errorf("cannot get dim names")
		}
	}
	a := c.dims.Name(name)
	if a == nil {
		return nil, fmt.Errorf("dim %s does not exixts", name)
	}
	d := a.(*Dim)
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (c *Cube) dim(id int) (*Dim, error) {
	if c.dims.Empty() {
		err := c.init()
		if err != nil {
			return nil, fmt.Errorf("cannot get dim id")
		}
	}
	a := c.dims.Id(id)
	if a == nil {
		return nil, fmt.Errorf("dim with id %d does not exixts (%v)", id, c.dims.Ids())
	}
	d := a.(*Dim)
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return nil, err
		}
	}
	return d, nil

}

func (c *Cube) analizeCoord(coord Coord) (bool, error) {
	if len(c.Data.Dimensions) != len(coord) {
		return false, fmt.Errorf("wrong length %d, expected %d", len(coord), len(c.dims.objects))
	}
	var isConsolidate bool
	for i, elId := range coord {
		dm, _ := c.dim(c.Data.Dimensions[i])
		el, err := dm.elem(elId)
		if err != nil {
			return false, err
		}
		if len(el.Children()) > 0 {
			isConsolidate = true
		}
	}
	return isConsolidate, nil
}

// Use the given coordinate to get a cellgroup formed by one cell.
func (c *Cube) Cell(coord Coord) (*CellGroup, error) {
	hasCons, err := c.analizeCoord(coord)
	if err != nil {
		return nil, err
	}
	cg := CellGroup{cube: c, hasCons: hasCons, cells: []Cell{Cell{Path: coord}}}
	return &cg, nil
}

// Use the given coordinates to get a cellgroup formed by more cells.
func (c *Cube) Cells(coords []Coord) (*CellGroup, error) {
	cg := CellGroup{cube: c}
	for i := range coords {
		hasCons, err := c.analizeCoord(coords[i])
		if err != nil {
			return nil, err
		}
		if hasCons {
			cg.hasCons = true
		}
		cg.cells = append(cg.cells, Cell{Path: coords[i]})
	}
	return &cg, nil
}

// Use the given area to get a cellgroup formed by the cells in the area.
func (c *Cube) CellGroup(coords CoordArea) (*CellGroup, error) {
	cg, err := coords.Split(c.Data.Dimensions)
	if err != nil {
		return nil, err
	}
	return c.Cells(cg)
}

func (c *Cube) String() string {
	return fmt.Sprintf("<cube id:%d name:%q dims:%d>", c.Data.Id, c.Data.Name, len(c.dims.Names()))
}

func (c *Cube) fixme() {
	var nameParts = strings.Split(c.Data.Name, "#")
	c.Data.Name = strings.Trim(nameParts[0], " ")
	if c.Data.Name == "" {
		c.Data.Name = "#" + strings.Trim(nameParts[1], " ")
	}
	c.tags = make(map[string]string)
	for _, info := range nameParts[1:] {
		i := strings.Index(info, " ")
		if i == 0 {
			c.hash = strings.Trim(info, " ")
			continue
		}
		if i < 0 {
			continue
		}
		key := strings.Trim(info[:i], " ")
		value := strings.Trim(info[i:], " ")
		c.tags[key] = value
	}
}
