package cube

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

// An error in the execution of a request
type PaloError struct {
	Code    int // Code 0 means an internal package error
	Name    string
	Message string
}

func (err *PaloError) Error() string {
	return fmt.Sprintf("%s (%v): %s", err.Name, err.Code, err.Message)
}

const codeInvalidSession = 1015

// Palo Server configuation
type Config struct {
	User, Pwd, Host, Port, Db string
}

type sampleData struct {
	Data struct {
		Id   int
		Name string
	}
}

type client struct {
	io.Writer
	conf    Config
	baseUrl string
	sid     string
	dbId    string
}

func newClient(conf Config, w io.Writer) (*client, error) {
	var c = client{Writer: w, conf: conf}
	c.baseUrl = fmt.Sprintf("http://%s:%s", c.conf.Host, c.conf.Port)
	err := c.Login()
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *client) Login() error {
	p := make(params)
	p.Add("user", c.conf.User)
	p.Add("password", c.conf.Pwd)
	rows, err := c.doRequest("/server/login", p)
	if err != nil {
		return err
	}
	var loginData struct {
		Data struct{ Session, Time string }
	}
	if err := rows[0].Unmarshal(&loginData); err != nil {
		return err
	}
	c.sid = loginData.Data.Session
	return nil
}

func (c *client) Write(text ...string) {
	if c.Writer != nil {
		for _, s := range text {
			c.Writer.Write([]byte(s))
		}
		c.Writer.Write([]byte("\n"))
	}
}

// Executes a request to palo and returns the rows.
func (c *client) doRequest(url string, p params) (result []resultRow, pe *PaloError) {
	if p == nil {
		p = make(params)
	}
	p.Set("sid", c.sid)
	p.Set("database", c.dbId)
	url = fmt.Sprintf("%s%s?%s", c.baseUrl, url, p.String())
	c.Write("Request: ", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, internalErr(fmt.Sprintf("request error: %s", err))
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, internalErr(fmt.Sprintf("body read error: %s", err))
	}
	c.Write("Response: ", string(data))
	if resp.StatusCode == 400 {
		rd, err := newResultRow(string(data))
		if err != nil {
			return nil, internalErr(fmt.Sprintf("bad row: %q", string(data)))
		}
		var pe struct {
			Data PaloError
		}
		err = rd.Unmarshal(&pe)
		if err != nil {
			return nil, internalErr(err.Error())
		}
		if pe.Data.Code == codeInvalidSession {
			err = c.Login()
			if err != nil {
				return nil, internalErr(fmt.Sprintf("login: %s", err))
			}
			return c.doRequest(url, p)
		}
		return nil, &pe.Data
	}
	resRows := strings.Split(string(data), "\n")
	if len(resRows) == 0 {
		return nil, internalErr(fmt.Sprintf("no rows found: %q", resRows))
	}
	if resRows[len(resRows)-1] == "" {
		resRows = resRows[:len(resRows)-1]
	}
	for i := range resRows {
		row, err := newResultRow(resRows[i])
		if err != nil {
			return nil, internalErr(fmt.Sprintf("row %d: %s", i, err.Error()))
		}
		result = append(result, row)
	}
	return result, nil
}

func (c *client) GetCube(cubeName string, isAttribute bool) (*Cube, error) {
	rows, err := c.doRequest("/server/databases", nil)
	if err != nil {
		return nil, fmt.Errorf("request error")
	}
	for i := 0; i < len(rows); i++ {
		var d sampleData
		err := rows[i].Unmarshal(&d)
		if err != nil {
			return nil, err
		}
		if c.conf.Db == d.Data.Name {
			c.dbId = strconv.Itoa(d.Data.Id)
			break
		}
	}
	if c.dbId == "" {
		return nil, fmt.Errorf("db %s not found", c.conf.Db)
	}
	p := params{}
	if isAttribute {
		p.Add("show_attribute", "1")
	}
	rows, err = c.doRequest("/database/cubes", p)
	if err != nil {
		return nil, fmt.Errorf("request error")
	}
	var cb Cube
	for i := 0; i < len(rows); i++ {
		err := rows[i].Unmarshal(&cb)
		if err != nil {
			return nil, err
		}
		if cubeName == cb.Data.Name {
			break
		}
	}
	if cubeName != cb.Data.Name {
		return nil, fmt.Errorf("cube %s not found", cubeName)
	}
	cb.isAttribute = isAttribute
	cb.client = c
	return &cb, nil
}
