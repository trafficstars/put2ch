package main

import (
	"net"
)

type udpReader struct {
	Conn *net.UDPConn
}

func (r *udpReader) Read(p []byte) (n int, err error) {
	n, _, _, _, err = r.Conn.ReadMsgUDP(p, nil)
	//n, _, err = r.Conn.ReadFrom(p)
	//n, err = r.Conn.Read(p)
	return
}

func (r *udpReader) Close() error {
	return r.Conn.Close()
}