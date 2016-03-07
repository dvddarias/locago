package main

import (
    "fmt"
    "os"
)

func main() {
	if len(os.Args) != 3 {
        showHelp();
	}
	service := os.Args[1]
    addr := os.Args[2]
    
    switch service {
    case "server":        
        server(addr);
        break;
    case "proxy":
        proxy(addr);
        break;
    default:
        showHelp();
    }
	os.Exit(0)
}

func showHelp(){
	fmt.Fprintf(os.Stderr, "Usage: %s [proxy|server] host:port\n", os.Args[0])
	os.Exit(1)
}