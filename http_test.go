package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bmizerany/pat"
)

var (
	server *httptest.Server

//	reader   io.Reader //Ignore this for now
//	usersUrl string
)

func init() {
	endpoint := HTTPEndpoint{}
	mux := pat.New()
	mux.Post(fmt.Sprintf("/:topic"), http.HandlerFunc(endpoint.postEvent))

	server = httptest.NewServer(mux) //Creating new server with the user handlers

}

func TestHTTPConfig(t *testing.T) {
}
