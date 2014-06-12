package cube

import (
	"errors"
	"fmt"
)

// An invalid attemp of setting or updating the value of a consolidate cell.
var ErrConsolidate = errors.New("cellgroup contains consolidate cells")

// A Palo Cell.
type Cell struct {
	Data struct {
		Type  int     //Type of the value (1=NUMERIC, 2=STRING)
		Exist int     //1 if at least one base cell for the path exists
		Value float64 //Value of the cell
	}
	Path Coord //cell coordinates
}

// A group of cells that can be manipulated or read.
type CellGroup struct {
	cube    *Cube
	cells   []Cell
	hasCons bool
}

// True if there is any consolidate cell.
func (cg *CellGroup) HasCons() bool {
	return cg.hasCons
}

// The cells of the group.
func (cg *CellGroup) Cells() []Cell {
	return cg.cells
}

// Fetch the cells values from the cube.
func (cg *CellGroup) Fetch() error {
	p := params{}
	for _, c := range cg.cells {
		p.Path("paths", []string{c.Path.String()})
	}
	rows, err := cg.cube.doRequest("/cell/values", p)
	if err != nil {
		return fmt.Errorf("cells: %s", err)
	}
	for i := range cg.cells {
		if err := rows[i].Unmarshal(&cg.cells[i]); err != nil {
			return fmt.Errorf("cell: bad row %d (%s)", i, err)
		}
	}
	return nil
}

func (cg *CellGroup) change(values []interface{}, add, bulk bool) error {
	if cg.hasCons {
		return ErrConsolidate
	}
	if l := len(values); bulk && l != 1 {
		return fmt.Errorf("values length %d, expected 1", l)
	} else if ex := len(cg.cells); !bulk && ex != l {
		return fmt.Errorf("values length %d, expected %d", l, ex)
	}
	p := params{}
	if add {
		p.Add("add", "1")
	}
	for i, c := range cg.cells {
		var j = 0
		if !bulk {
			j = i
		}
		p.Path("values", []string{fmt.Sprintf("%v", values[j])})
		p.Path("paths", []string{c.Path.String()})
	}
	rows, err := cg.cube.doRequest("/cell/replace_bulk", p)
	if err != nil {
		return fmt.Errorf("cells: %s", err)
	}
	var badrows []int
	for i := range rows {
		if !rows[i].HasField(0) || rows[i][0].String() != "1" {
			badrows = append(badrows, i)
		}
	}
	if len(badrows) > 0 {
		return fmt.Errorf("cannot insert rows %v", badrows)
	}
	return nil
}

// Sets a value for each cell.
func (cg *CellGroup) Set(values []interface{}) error {
	return cg.change(values, false, false)
}

// Sets the same value for all cells.
func (cg *CellGroup) SetAll(value interface{}) error {
	return cg.change([]interface{}{value}, false, true)
}

//Add a value to each cell.
func (cg *CellGroup) Add(values []interface{}) error {
	return cg.change(values, true, false)
}

// Adds the same value to all cells.
func (cg *CellGroup) AddAll(value interface{}) error {
	return cg.change([]interface{}{value}, true, true)
}

// Joins two cellgroups into one.
func (cg *CellGroup) Append(c CellGroup) *CellGroup {
	return &CellGroup{
		cube:    cg.cube,
		cells:   append(cg.cells, c.Cells()...),
		hasCons: cg.hasCons || c.HasCons(),
	}
}

func (cg *CellGroup) String() string {
	return fmt.Sprintf("<cellgroup cells:%d canset:%v/>", len(cg.cells), !cg.hasCons)
}
