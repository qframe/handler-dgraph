package qhandler_dgraph

type Container struct {
	ID       string     `json:"id,omitempty"`
	Name     string     `json:"name,omitempty"`
	Image    string     `json:"image,omitempty"`
}

type Network struct {
	ID       string     `json:"id,omitempty"`
	Name     string     `json:"name,omitempty"`
	Driver   string     `json:"driver,omitempty"`
	Scope    string     `json:"scope,omitempty"`
}