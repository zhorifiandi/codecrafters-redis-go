package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type RedisStore struct {
	Data map[string]string
}

// Implement Redis Protocol Parser
type RedisRequest struct {
	Command string
	Args    []string
}

func parseRequest(request string) RedisRequest {
	// Get the number of arguments, we parse number after * until \r\n
	// Iterate over the request and parse the arguments
	var arguments []string
	for i := 0; i < len(request); i++ {
		if request[i] == '$' {
			// Get the length of the argument
			for j := i + 1; j < len(request); j++ {
				if request[j] == '\r' {
					argLength, _ := strconv.Atoi(request[i+1 : j])
					i = j + 2
					arguments = append(arguments, request[j+2:j+2+argLength])
					break
				}
			}
		}
	}

	return RedisRequest{
		Command: arguments[0],
		Args:    arguments[1:],
	}

}

func constructEchoResponse(args []string) string {
	responseStringArray := []string{}
	for _, arg := range args {
		responseStringArray = append(responseStringArray, fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}

	response := strings.Join(responseStringArray, "")
	fmt.Println("Response: ", response)
	return response
}

func handleSetCommand(store *RedisStore, args []string) error {
	store.Data[args[0]] = args[1]
	return nil
}

func handleGetCommand(store *RedisStore, key string) (string, error) {
	value, ok := store.Data[key]
	if !ok {
		return "", fmt.Errorf("Key not found")
	}
	return value, nil
}

func handleSingleConnection(c net.Conn, store *RedisStore) {
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
		fmt.Println("Incoming Request: ", incomingRawRequest)
		parsedRequest := parseRequest(incomingRawRequest)
		fmt.Println("Parsed Request: ", parsedRequest)

		switch parsedRequest.Command {
		case "PING":
			c.Write([]byte("+PONG\r\n"))
		case "QUIT":
			c.Write([]byte("+OK\r\n"))
			c.Close()
			return
		case "ECHO":
			echoResponse := constructEchoResponse(parsedRequest.Args)
			c.Write([]byte(echoResponse))
		case "SET":
			if len(parsedRequest.Args) != 2 {
				c.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			err := handleSetCommand(store, parsedRequest.Args)
			if err != nil {
				c.Write([]byte("-ERR " + err.Error() + "\r\n"))
			} else {
				c.Write([]byte("+OK\r\n"))
			}
		case "GET":
			if len(parsedRequest.Args) != 1 {
				c.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			value, err := handleGetCommand(store, parsedRequest.Args[0])
			if err != nil {
				c.Write([]byte("-ERR " + err.Error() + "\r\n"))
			} else {
				c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			}
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

	defer l.Close()

	store := RedisStore{
		Data: make(map[string]string),
	}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleSingleConnection(c, &store)
	}
}
