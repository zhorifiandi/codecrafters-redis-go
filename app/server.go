package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type RedisStoreDataItem struct {
	Value            string
	CreatedTimestamp time.Time
	Expiry           int
	ExpiredTimestamp *time.Time
}

type RedisStore struct {
	Data map[string]RedisStoreDataItem
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

	upperCaseCommand := strings.ToUpper(arguments[0])
	return RedisRequest{
		Command: upperCaseCommand,
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

func handleSetCommand(store *RedisStore, args []string) (err error) {
	var expiryTime int
	// Handle expiry (PX argument)
	if len(args) == 4 {
		// Check if the argument is PX
		if strings.ToUpper(args[2]) == "PX" {
			// Get the expiry time
			expiryTime, err = strconv.Atoi(args[3])
			if err != nil {
				return fmt.Errorf("Invalid expiry time")
			}
		} else {
			return fmt.Errorf("Invalid argument")
		}
	}

	// Determine expired timestamp
	var expiredTimestamp *time.Time
	if expiryTime != 0 {
		e := time.Now().Add(time.Duration(expiryTime) * time.Millisecond)
		expiredTimestamp = &e
	}

	store.Data[args[0]] = RedisStoreDataItem{
		Value:            args[1],
		CreatedTimestamp: time.Now(),
		Expiry:           expiryTime,
		ExpiredTimestamp: expiredTimestamp,
	}

	return nil
}

var KEY_HAS_EXPIRED_ERROR = fmt.Errorf("KEY_HAS_EXPIRED_ERROR")
var ENCODED_NULL_BYTE = []byte("$-1\r\n")

func handleGetCommand(store *RedisStore, key string) (string, error) {
	value, ok := store.Data[key]
	if !ok {
		return "", nil
	}

	// Check if the key has expired
	if value.ExpiredTimestamp != nil {
		if time.Now().After(*value.ExpiredTimestamp) {
			delete(store.Data, key)
			return "", KEY_HAS_EXPIRED_ERROR
		}
	}

	return value.Value, nil

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
				if err == KEY_HAS_EXPIRED_ERROR {
					c.Write(ENCODED_NULL_BYTE)
				} else {
					c.Write([]byte("-ERR " + err.Error() + "\r\n"))
				}
			} else {
				c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			}
		default:
			c.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func main() {
	port := flag.String("port", "6379", "Port to bind to")
	flag.Parse()

	address := fmt.Sprintf("0.0.0.0:%s", *port)

	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	store := RedisStore{
		Data: make(map[string]RedisStoreDataItem),
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
