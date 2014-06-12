package cube

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const _LABEL = "label"

// Error type for a non existing element.
type ErrMissElem [2]string

func (e ErrMissElem) Error() string {
	return fmt.Sprintf("element %q missing in dimension %q", e[1], e[0])
}

// A Cube dimension.
type Dim struct {
	cube  *Cube
	tags  map[string]string
	elems cache
	roots cache
	Data  struct {
		Id        int    // Identifier of the dimension
		Name      string // Name of the dimension
		Elements  int    // Number of elements
		MaxLvl    int    // Maximum level of the dimension
		MaxIndent int    // Maximum indent of the dimension
		MaxDepth  int    // Maximum depth of the dimension
		Type      int    // Type of dimension (0=normal, 1=system, 2=attribute, 3=user info)
		DimAttr   int    // Identifier of the attributes dimension of a normal dimension
		// or the identifier of the normal dimension associated to a attributes dimension
		CubeAttr   int // Identifier of the attributes cube. (only for normal dimensions)
		CubeRights int // Identifier of the rights cube. (only for normal dimensions)
		DimToken   int // The dimension token of the dimension
	}
}

func (d *Dim) init() error {
	err := d.initElems()
	if err != nil {
		return err
	}
	return nil
}

func (d *Dim) initElems() error {
	d.elems = newCache()
	d.roots = newCache()
	p := make(params)
	p.Add("dimension", strconv.Itoa(d.Data.Id))
	rows, err := d.cube.doRequest("/dimension/elements", p)
	if err != nil {
		return fmt.Errorf("elems init: %s", err)
	}
	for i := 0; i < len(rows); i++ {
		var el Elem
		err := rows[i].Unmarshal(&el)
		if err != nil {
			return fmt.Errorf("elems init: bad row %d (%s)", i, err)
		}
		d.elems.Add(&el)
	}
	for _, key := range d.elems.Ids() {
		el := d.elems.Id(key).(*Elem)
		err := el.init(&d.elems)
		if err != nil {
			return err
		}
		if len(el.parents) == 0 {
			d.roots.Add(el)
		}
	}
	return nil
}

// Return the dimension Id.
func (d *Dim) Id() int {
	return d.Data.Id
}

// Return the dimension Name.
func (d *Dim) Name() string {
	return d.Data.Name
}

// Gives the elements name list.
func (d *Dim) ElemNames() ([]string, error) {
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return nil, err
		}
	}
	return d.elems.Names(), nil
}

// Return an element by its name.
func (d *Dim) Elem(name string) (*Elem, error) {
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return nil, fmt.Errorf("cannot get elems names")
		}
	}
	a := d.elems.Name(name)
	if a == nil {
		return nil, &ErrMissElem{d.Data.Name, name}
	}
	return a.(*Elem), nil
}

func (d *Dim) elem(id int) (*Elem, error) {
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return nil, fmt.Errorf("cannot get elems id")
		}
	}
	a := d.elems.Id(id)
	if a == nil {
		return nil, fmt.Errorf("elem with id %d does not exists in dimension %s", id, d.Data.Name)
	}
	e := a.(*Elem)
	return e, nil
}

// Gives the root elements name list.
func (d *Dim) RootNames() ([]string, error) {
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return nil, err
		}
	}
	return d.roots.Names(), nil
}

// Adds a new element to the dimension, a root if parent is empty.
func (d *Dim) AddElem(name, parentName string, cons bool, label string) error {
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return err
		}
	}
	var parent *Elem
	var err error
	if parentName != "" {
		parent, err = d.Elem(parentName)
		if err != nil {
			return err
		}
	}
	p := params{}
	p.Add("dimension", strconv.Itoa(d.Data.Id))
	t := "2"
	if cons {
		t = "4"
	}
	p.Add("type", t)
	p.Add("new_name", url.QueryEscape(name))
	row, pErr := d.cube.doRequest("/element/create", p)
	if pErr != nil {
		return pErr
	}
	var el Elem
	err = row[0].Unmarshal(&el)
	if err != nil {
		return err
	}
	if parentName != "" {
		p := params{}
		p.Add("dimension", strconv.Itoa(d.Data.Id))
		p.Add("element", strconv.Itoa(parent.Id()))
		p.Add("children", strconv.Itoa(el.Id()))
		_, pErr := d.cube.doRequest("/element/append", p)
		if pErr != nil {
			return pErr
		}
	}
	err = d.init()
	if err != nil {
		return err
	}
	if label != "" {
		return d.elemLabel(name, label)
	}
	return nil
}

func (d *Dim) elemLabel(name, label string) error {
	n := "#_" + d.Name()
	cube, err := d.cube.client.GetCube(n, true)
	if err != nil {
		return fmt.Errorf("cannot get label cube: %s", err)
	}
	dim, err := cube.Dim(n)
	if err != nil {
		return fmt.Errorf("cannot get label dimension: %s", err)
	}
	dim.AddElem(_LABEL, "", false, "")

	if _, err = dim.Elem(_LABEL); err != nil {
		return fmt.Errorf("cannot create/find label element: %s", err)
	}
	coord, err := cube.Coords(map[string]string{n: _LABEL, d.Name(): name})
	if err != nil {
		return fmt.Errorf("cannot get label coords: %s", err)
	}
	cell, err := cube.Cell(coord[0])
	if err != nil {
		return fmt.Errorf("cannot get label cell: %s", err)
	}
	err = cell.SetAll(label)
	if err != nil {
		return fmt.Errorf("cannot set label value: %s", err)
	}
	return nil
}

// Removes element from the dimension.
func (d *Dim) DelElem(name string) error {
	if d.elems.Empty() {
		err := d.init()
		if err != nil {
			return err
		}
	}
	el, err := d.Elem(name)
	if err != nil {
		return err
	}
	p := params{}
	p.Add("dimension", strconv.Itoa(d.Data.Id))
	p.Add("element", strconv.Itoa(el.Id()))
	rows, pErr := d.cube.doRequest("/element/destroy", p)
	if pErr != nil {
		return pErr
	}
	var newel Elem
	err = rows[0].Unmarshal(&newel)
	if err != nil {
		return err
	}
	err = d.init()
	if err != nil {
		return err
	}
	return nil
}

func (d *Dim) String() string {
	return fmt.Sprintf("<dim id:%d name:%q tags:%q elems:%d>", d.Data.Id, d.Data.Name, d.tags, d.elems.Size())
}

func (d *Dim) fixme() {
	var nameParts = strings.Split(d.Data.Name, "#")
	d.Data.Name = strings.Trim(nameParts[0], " ")
	if d.Data.Name == "" {
		d.Data.Name = "#" + strings.Trim(nameParts[1], " ")
	}
	d.tags = make(map[string]string)
	for _, info := range nameParts[1:] {
		i := strings.Index(info, " ")
		if i <= 0 {
			continue
		}
		key := strings.Trim(info[:i], " ")
		value := strings.Trim(info[i:], " ")
		d.tags[key] = value
	}
}
