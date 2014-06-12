# Cube

Package *cube* implements a client for [Palo Olap server](http://sourceforge.net/projects/palo/).

It allows the user to execute operations using names dimensions/elements name, unaware of the server generated ids.


## Basic Usage

### Creating a cube:
	var cubename = "Cube"
	cube, err := cube.New(cubename, Config{
		User: "username", Pwd: "password", Host: "localhost", Port: "7777", Db: "DbName",
	})
	if err != nil {
		fmt.Println("Cannot create cube:", err)
		return
	}
	dims, err := cube.DimNames()
	if err != nil {
		fmt.Println("Cannot retrieve dimensions name:", err)
		return
	}
	fmt.Println(cubename, dims)

### Using a dimension:
	var dimname = "Dimension"
	dim, err := cube.Dim(dimname)
	if err != nil {
		fmt.Println("Cannot get dim:", err)
		return
	}
	roots, err := dim.RootNames()
	if err != nil {
		fmt.Println("Cannot get root names:", err)
		return
	}
	fmt.Println(dimname, roots)

### Using a element:
	var elname = "Element"
	el, err := dim.El(elname)
	if err != nil {
		fmt.Println("Cannot get el:", err)
		return
	}
	fmt.Println(elname, "parents", e.Parents())
	fmt.Println(elname, "children", e.Children())

### Getting and updating cell value:
	var m = make(map[string]string)
	for _, k := range dimnames {
		m[k] = "ALL"
	}
	coords, err := cube.Coords(m)
	if err != nil {
		fmt.Println("Invalid coords:", err)
		return
	}
	cells, err := cube.Cells([]Coord)
	if err != nil {
		fmt.Println("Cannot get cells:", err)
		return
	}
	err = cells.Fetch()
	if err != nil {
		fmt.Println("Cannot fetch cells value:", err)
		return
	}
	for _, c := cells.Cells() {
		fmt.Println(c, c.Data.Value)
	}
	err = cells.AddAll(1)
	if err != nil {
		fmt.Println("Cannot increment cells value:", err)
		return
	}

See [documentation](http://godoc.org/github.com/klaidliadon/cube) for help.

