package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/sidquark/KeyValueDatabase/internal/database"
)

func main() {
	fmt.Println("Welcome to Key-Value Database")
	fmt.Println("Starting database...")
	
	// Create database with default configuration
	db, err := database.New(nil)
	if err != nil {
		fmt.Printf("Error initializing database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	
	fmt.Println("Database started successfully.")
	fmt.Println("Type 'help' for available commands.")
	
	// Start command loop
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		
		input := scanner.Text()
		
		if input == "exit" || input == "quit" {
			break
		}
		
		processCommand(db, input)
	}
	
	fmt.Println("Shutting down database...")
}

func processCommand(db *database.DB, input string) {
	parts := strings.Split(input, " ")
	if len(parts) == 0 {
		return
	}
	
	command := strings.ToLower(parts[0])
	
	switch command {
	case "set":
		if len(parts) < 3 {
			fmt.Println("Usage: SET key value")
			return
		}
		key := parts[1]
		value := []byte(strings.Join(parts[2:], " "))
		err := db.Set(key, value)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println("OK")
		}
		
	case "get":
		if len(parts) != 2 {
			fmt.Println("Usage: GET key")
			return
		}
		key := parts[1]
		value, err := db.Get(key)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("%s\n", value)
		}
		
	case "delete":
		if len(parts) != 2 {
			fmt.Println("Usage: DELETE key")
			return
		}
		key := parts[1]
		err := db.Delete(key)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println("OK")
		}
		
	case "keys":
		keys := db.Keys()
		if len(keys) == 0 {
			fmt.Println("(empty database)")
		} else {
			for _, key := range keys {
				fmt.Println(key)
			}
		}
		
	case "size":
		size := db.Size()
		fmt.Printf("Database size: %d entries\n", size)
		
	case "help":
		printHelp()
		
	default:
		fmt.Println("Unknown command. Type 'help' for available commands.")
	}
}

func printHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  SET key value   - Store a key-value pair")
	fmt.Println("  GET key         - Retrieve a value by key")
	fmt.Println("  DELETE key      - Remove a key-value pair")
	fmt.Println("  KEYS            - List all keys")
	fmt.Println("  SIZE            - Show database size")
	fmt.Println("  HELP            - Show this help")
	fmt.Println("  EXIT/QUIT       - Exit the program")
}
