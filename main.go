package main

import (
	"fmt"
	"net"
	"strings"

	aof2 "redis-clone-in-go/aof"
	handler2 "redis-clone-in-go/handler"
	"redis-clone-in-go/resp"
)

type ServerConnection struct {
	con net.Conn
}

func main() {
	fmt.Println("Listening on port :6379")

	// Create a new server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := aof2.NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aof.Read(func(value resp.Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		handler, ok := handler2.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		serverCon := &ServerConnection{
			con: conn,
		}

		go serverCon.Handle(aof)
	}
}

func (sc *ServerConnection) Handle(aof *aof2.Aof) {
	defer func() {
		_ = sc.con.Close()
	}()
	for {
		res := resp.NewResp(sc.con)
		value, err := res.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		writer := resp.NewWriter(sc.con)

		handler, ok := handler2.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(resp.Value{Typ: "string", Str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}
