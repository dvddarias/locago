package main

import (
    "github.com/fsouza/go-dockerclient"
	"encoding/json"
	"strconv"
	"strings"
	"bufio"
	"time"
	"log"
	"fmt"
	"net"
	"io"
    "os"
)

func proxy(addr string) {    
    
    var endpoint = "unix:///var/run/docker.sock";
    var client, _ = docker.NewClient(endpoint);
    log.Println("Started Docker log proxy.");
    
    var events = make(chan *docker.APIEvents);  
    var done = make(chan bool);
    
    //remove event listener on exit
    defer func() {
		var err = client.RemoveEventListener(events)
		if err != nil {
			log.Fatal(err)
		}

	}()
    
    
    log.Println("Registering for start event.");
    client.AddEventListener(events);
    go func() {
        var event *docker.APIEvents;
        //get the container on start event and redirect its log
        for event = range events {            
            if event.Status == "start"{
                if container := getContainerData(client, event.ID); container!=nil {
                    if container.State.Running{
                        log.Println(container.Name + " started.");
                        go redirectLogs(client, container, addr);                                
                    }      
                }
            }
        }
        done<-true;
    }()
            
    log.Println("Fetching running containers.");
    var containers, _ = client.ListContainers(docker.ListContainersOptions{All:true });
    
    //get the containes that were running on start & redirect their logs
    for _, c := range containers {
        var container = getContainerData(client, c.ID);                
        if (container.State.Running){                       
            log.Println(container.Name + " existed on start.");
            go redirectLogs(client, container, addr);            
        }
    }
    
    //wait for the event listener to end.
    <-done;
}

func getContainerData(client *docker.Client , ID string) *docker.Container {
    var container, _ = client.InspectContainer(ID);
    return container;
}

type LogData struct {
    Log string
    ContainerName string
    ContainerID string
    Timestamp string   
    Hostname string 
}

func redirectLogs(client *docker.Client, container *docker.Container, addr string){
    log.Println("Redirecting logs from " + container.Name);
    
    //pipe to write the logs and read them to build the json
    var r,w = io.Pipe();    
    //channel to write the json and then read it to send it
    var logs = make(chan string, 1000);
    
    //(docker) -> r,w pipe -> (log to json) -> logs chan -> (send to server)
    
    //goroutine that consumes the log channel and sends to the logs
    go sendLogs(logs, container.Name, addr);
    
    //goroutine that gets the string from the pipe and fills the json channel
    go func() {
        //setup the scanner
        scanner := bufio.NewScanner(r)
        hostname, _ := os.Hostname();
        for scanner.Scan() {
            var logLine = scanner.Text();  
            
            
            jsonData, _ := json.Marshal( LogData{
                Log: logLine, 
                ContainerName: strings.Trim(container.Name, "/ "),
                ContainerID: container.ID,
                Timestamp: strconv.FormatInt(time.Now().Unix(),10),  
                Hostname: hostname,            
            });

            logs <- string(jsonData);
        }        
        close(logs);
        log.Println(container.Name + " closed the log pipe." );
        
        if err := scanner.Err(); err != nil {
            log.Printf("There was an error in attached container %v\n", err)
        }        
    }()
    
    //goroutine that gets the docker logs and writes to the pipe
    go func() {
        var err = client.Logs(docker.LogsOptions{
            Container:container.ID, 
            Follow:true, 
            Stderr:true,
            Stdout: true,
            OutputStream: w,
            ErrorStream: w,
            Since: time.Now().Unix(),
        });
        
        //close the pipe to signal the scanner to stop        
        w.Close();
        
        if err != nil {
            log.Fatal(err)
        }        
    }()
}

func sendLogs(logs chan string, name string, addr string){
    var conn net.Conn;
    for logLine := range logs{
        for {
            if conn==nil{
                log.Println(name + " connecting to server.");
                conn = getServerConnection(addr);
                log.Println(name + " connected.");
            }
            
            var sent, err = fmt.Fprintf(conn, logLine+"\n");
            
            
            
            if err != nil || sent==0 {
                log.Println(name + " disconected from server.")
                conn.Close();
                //there was an error so try to connect again
                conn = nil;                  
                continue;              
            }
            //no errors so             
            break;
        }
    } 
    if conn!=nil{
        log.Println(name + " disconected from server.")
        conn.Close();
    }   
}

func getServerConnection(addr string) net.Conn {
    var conn net.Conn;
    var err error;
    for {
        conn, err = net.Dial("tcp", addr)
        if err != nil {
            log.Printf("Error connectiong to server: %v", err);
            var seconds time.Duration = 10;
            log.Printf("Reconnecting in %d seconds", seconds);
            time.Sleep(time.Second * seconds);
            continue;
        }        
        break;
    }         
    return conn;   
}
