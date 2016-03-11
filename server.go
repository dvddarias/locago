package main

import (
    "github.com/franela/goreq"
	"encoding/json"
    "os/signal"
	"net/url"
	"os/exec"
    "syscall"
	"time"
    "sync"
	"log"
	"net"
	"fmt"
    "os"
)

//varianbles holdig a reference to DB and Srv Process.
var tiedotProcess *exec.Cmd;
var caddyProcess *exec.Cmd;

//concurrent set of collection names (one per hostname)
var collections = struct{
    sync.RWMutex
    data map[string]bool
}{data: make(map[string]bool)}

func server(addr string) {
    
    dbAddr := "localhost:7778";    
    caddyAddr := "localhost:7778";
    serverAddr := addr;
        
    setupSignals(dbAddr, caddyAddr);
    setupTiedotDB(dbAddr);
    setupCaddy(caddyAddr);
    
    go scheduledCleaner();
    
    //listen for incomming logs    
	var listener, err = net.Listen("tcp", serverAddr);      
	checkError(err)
    
    log.Println("Server listening on " + serverAddr);

	for {
		var conn, err = listener.Accept()
		if err != nil {            
            continue;
		}
		// handle the client
		go handleClient(conn, dbAddr)
	}
}

func scheduledCleaner(){
    var timer = make(chan bool);
    go func ()  {
        for{
            time.Sleep(time.Hour*24*1);
            timer <- true;
        }
    }()
    for range timer{
        cleanDB();
    }
}

func cleanDB(){
    //TODO
    log.Println("Running scheduled cleanning.");
}

func setupCaddy(addr string){
    runCaddy(addr);
}

func runCaddy(addr string){
    runProcess("caddy", &caddyProcess);
}

func runTiedotDB(dbAddr string){
    runProcess("tiedot", &tiedotProcess, "-mode=httpd", "-dir=/tmp/locagodb", "-port=7778", "-bind=0.0.0.0", "-verbose");
}

func setupTiedotDB(dbAddr string){
    //run the server
    runTiedotDB(dbAddr);
    //query the collection names
    var res *goreq.Response;
    var err error;
    
    for{
        log.Println("Quering collections.")  
        res, err = goreq.Request{ Uri: "http://"+dbAddr+"/all" }.Do();
        if err==nil && res.Response.StatusCode==200{ break; }         
        time.Sleep(time.Second*1);   
    }    
    //load collections from the list
    var cols = make([]string,0);
    res.Body.FromJsonTo(&cols);
    
    //add existing collection to the list
    for _, c := range cols{
        collections.data[c]= true;
    }
       
    log.Printf("Existing collections %v\n", collections.data);  
}


func runProcess(name string, process **exec.Cmd, arg ...string){
    (*process) = exec.Command(name, arg...);
    (*process).Stdout = os.Stdout;
    (*process).Stderr = os.Stderr;
    (*process).Start();
}

func setupSignals(dbAddr string, serverAddr string){
    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGCHLD)
    
    go func() {        
        for sig := range sigs{
            
            var wstatus syscall.WaitStatus;            
            var pid, _ = syscall.Wait4(-1, &wstatus, syscall.WNOHANG, nil);
            
            switch sig {
                case syscall.SIGCHLD:
                    if pid == tiedotProcess.Process.Pid{
                        log.Println("Restarting Tidot DB.");
                        tiedotProcess.Wait();
                        runTiedotDB(dbAddr);
                    }
                    if pid == caddyProcess.Process.Pid{
                        log.Println("Restarting Caddy Server.");
                        caddyProcess.Wait();
                        runCaddy(serverAddr);
                    }       
                    break;                             
                default:
                    log.Println("Closing Tidot DB.");
                    goreq.Request{ Uri: "http://"+dbAddr+"/shutdown" }.Do();
                    tiedotProcess.Wait()
                    
                    log.Println("Closing Caddy Server.");
                    caddyProcess.Process.Kill();
                    caddyProcess.Wait()
                    log.Println("Bye.")
                    os.Exit(0);            
            }
        }
    }()
}

func handleClient(conn net.Conn, dbAddr string) {
    var sendToDB = make(chan LogData, 1000);  
	
    // close connection and channel on exit
	defer func ()  {
        conn.Close();
        close(sendToDB);
        log.Println("Container disconnected.");        
    }()  
        
    go func ()  {
        for json := range sendToDB{
            storeLog(json, dbAddr);
        }
    } ()
    
    log.Println("Container connected.");
    
    decoder := json.NewDecoder(conn);    
    var logLine LogData;
    
    for {
        if err := decoder.Decode(&logLine); err != nil {
            return;            
        }        
        sendToDB <- logLine;
    }
}

func storeLog(logLine LogData, dbAddr string){
    
    //one collection per host logging.
    var colName = logLine.Hostname;
    query := url.Values{}
    query.Set("col", colName )
    
    collections.Lock();
    _, inside := collections.data[colName]
    collections.Unlock();
    
    if !inside {        
        res, err := goreq.Request{
            Uri: "http://"+dbAddr+"/create",
            QueryString: query,
        }.Do();
        
        if err==nil && res.Response.StatusCode==201 {
            log.Println("Created new collection named "+ colName);
            collections.Lock();
            collections.data[colName] = true;
            collections.Unlock();
        }
    }
    
    uri := "http://"+dbAddr+"/insert";
    
    data, _ := json.Marshal(logLine);
    jsonData := string(data);
    
    var attempts = 0;
        
    for{
        res, err := goreq.Request{
            Method: "POST",
            Uri: uri,
            QueryString: query,
            ContentType: "application/x-www-form-urlencoded",
            Body: "doc="+jsonData,
            
        }.Do();
        
        //only get out if no error and 201 code or three failed attempts
        if attempts==3 || (err==nil && res.Response.StatusCode==201) {
            break;
        }
        
        attempts++;
        time.Sleep(time.Second*2)
        log.Printf("Failed to store on the database. %v %d. retrying...\n", err, res.Response.StatusCode);
    }
    
    log.Printf("Stored on db \n%v\n", jsonData);
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s\n", err.Error())
		os.Exit(1)
	}
}

