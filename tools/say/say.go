package main

import "fmt"

type Sth struct {
	msg string
}

func (s *Sth) NewSth() {
	s.msg = "ala"
	return
}

func (s *Sth) SetSth() {
	s.msg = "set sth"
	return
}

func (s *Sth) Say() {
	fmt.Println(s.msg)
	return
}

func main() {
	var sth1 Sth

	// sth1 := Sth{"constructor"}
	fmt.Println("sth1 - NewSth")
	sth1.NewSth()
	sth1.Say()
	fmt.Println("sth1 - SetSth")
	sth1.SetSth()
	sth1.Say()
	fmt.Println("sth1 - outside")

	sth1.msg = "outside"
	sth1.Say()

	fmt.Println("Teraz - sth2")
	sth2 := new(Sth)
	sth2.Say()
}
