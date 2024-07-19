package main

import (
	"fmt"
	"net"
	"os"
)

// TODO: Implement a function that parses the incoming request and returns the command
func parseCommand(rawRequest string) string {
	switch rawRequest {
	case "*1\r\n$4\r\nPING\r\n":
		return "PING"
	case "*1\r\n$4\r\nQUIT\r\n":
		return "QUIT"
	default:
		return rawRequest
	}
}

func handleSingleConnection(c net.Conn) {
	for {
		buf := make([]byte, 1024)
		byteSize, err := c.Read(buf)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Connection closed from client.")
				c.Close()
				return
			} else {
				fmt.Println("Error reading: ", err.Error())
				return
			}
		}

		incomingRawRequest := string(buf[:byteSize])
		fmt.Println("Received Request: ", incomingRawRequest)

		parsedCommand := parseCommand(incomingRawRequest)
		switch parsedCommand {
		case "PING":
			c.Write([]byte("+PONG\r\n"))
		case "QUIT":
			c.Write([]byte("+OK\r\n"))
			c.Close()
			return
		default:
			c.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleSingleConnection(c)
	}
}
