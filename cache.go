package cube

// A cointainer for `indexable`, searchable by name and id
type cache struct {
	objects map[int]indexable
	names   map[string]int
}

type indexable interface {
	Id() int
	Name() string
}

func newCache() cache {
	var c = cache{}
	c.objects = make(map[int]indexable)
	c.names = make(map[string]int)
	return c
}

func (c *cache) Add(v indexable) {
	id, name := v.Id(), v.Name()
	c.objects[id] = v
	c.names[name] = id
}

func (c *cache) Id(id int) indexable {
	return c.objects[id]
}

func (c *cache) Name(name string) indexable {
	id, ok := c.names[name]
	if !ok {
		return nil
	}
	return c.Id(id)
}

func (c *cache) Ids() []int {
	var r []int
	for _, v := range c.objects {
		r = append(r, v.Id())
	}
	return r
}

func (c *cache) Names() []string {
	var r []string
	for _, v := range c.objects {
		r = append(r, v.Name())
	}
	return r
}

func (c *cache) Empty() bool {
	return len(c.objects) == 0
}

func (c *cache) Size() int {
	return len(c.objects)
}
