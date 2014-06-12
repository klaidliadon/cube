package cube

import (
	"fmt"
	"strings"
)

// a map that becomes a GET querystring (array values become a csv string).
type params map[string]par

type par struct {
	data   []string
	joiner string
}

func (p *params) append(k string, v ...string) {
	pr := (*p)[k]
	pr.data = append(pr.data, v...)
	(*p)[k] = pr
}

func (p *params) Add(k string, v ...string) {
	if _, ok := (*p)[k]; !ok {
		(*p)[k] = par{joiner: ","}

	}
	p.append(k, v...)
}

func (p *params) Set(k string, v string) {
	(*p)[k] = par{data: []string{v}}
}

func (p *params) Path(k string, v ...[]string) {
	if _, ok := (*p)[k]; !ok {
		(*p)[k] = par{joiner: ":"}
	}
	var values []string
	for _, s := range v {
		values = append(values, strings.Join(s, ","))
	}
	p.append(k, values...)
}

func (p params) String() string {
	var s []string
	for k, v := range p {
		s = append(s, fmt.Sprintf("%s=%s", k, strings.Join(v.data, v.joiner)))
	}
	return strings.Join(s, "&")
}
