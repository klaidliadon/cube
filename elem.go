package cube

import (
	"fmt"
)

// A dimension element.
type Elem struct {
	parents  []*Elem
	children []*Elem
	Data     struct {
		Id              int    //Identifier of the element
		Name            string //Name of the element
		Position        int    //Position of the element
		Level           int    //Level of the element
		Indent          int    //Indent of the element
		Depth           int    //Depth of the element
		Type            int    //Type of the element (1=NUMERIC, 2=STRING, 4=CONSOLIDATED)
		Number_parents  int    //Number of parents
		Parents         []int  //Comma separate list of parent identifiers
		Number_children int    //Number of children
		Children        []int  //Comma separate list of children identifiers
		Weights         []int  //Comma separate list of children weight
	}
}

func (e *Elem) init(c *cache) error {
	for _, key := range e.Data.Parents {
		e.parents = append(e.parents, c.Id(key).(*Elem))
	}
	for _, key := range e.Data.Children {
		e.children = append(e.children, c.Id(key).(*Elem))
	}
	return nil
}

func (e *Elem) Id() int {
	return e.Data.Id
}

func (e *Elem) Name() string {
	return e.Data.Name
}

func (e *Elem) String() string {
	return fmt.Sprintf("<elem id:%d name:%q parents:%v children:%v>", e.Data.Id, e.Data.Name, len(e.parents), len(e.children))
}

//Return element parents.
func (e *Elem) Parents() []*Elem {
	return e.parents
}

//Return element children.
func (e *Elem) Children() []*Elem {
	return e.children
}

func (e *Elem) newChild(el *Elem) {
	e.children = append(e.children, el)
}
