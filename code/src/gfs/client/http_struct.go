package client

type CreateRequest struct {
	Path string
}
type CreateResponse struct {
	Success bool
	Error string
}

type DeleteRequest struct {
	Path string
}
type DeleteResponse struct {
	Success bool
	Error string
}

type RenameRequest struct {
	Source string
	Target string
}
type RenameResponse struct {
	Success bool
	Error string
}

type MkdirRequest struct {
	Path string
}
type MkdirResponse struct {
	Success bool
	Error string
}

type ReadRequest struct {
	Path string
	Offset int
	Length int
}
type ReadResponse struct {
	Success bool
	Error string
	Data string
}

type WriteRequest struct {
	Path string
	Offset int
	Data string
}
type WriteResponse struct {
	Success bool
	Error string
	Size int
}

type AppendRequest struct {
	Path string
	Data string
}
type AppendResponse struct {
	Success bool
	Error string
	Offset int
}