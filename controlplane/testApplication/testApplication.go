package main

import (
	"flag"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type testApplication struct {
	portRange string
}

func (handler *testApplication) getCmdLineArgs() {
	flag.StringVar(&handler.portRange, "p", "NULL", "Port range [0-9]")
}

func forever() {
	for {
		time.Sleep(time.Second)
	}
}

func (handler *testApplication) startHttpPort() {
	parts := strings.Split(handler.portRange, "-")
	if len(parts) < 2 {
		log.Error("Invalid port range format. Expected 'start-end'")
		return
	}

	portRangeStart, err := strconv.Atoi(parts[0])
	if err != nil {
		log.Error("Error parsing start port: ", err)
		return
	}

	portRangeEnd, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Error("Error parsing end port: ", err)
		return
	}

	for i := portRangeStart; i <= portRangeEnd; i++ {
		mux := http.NewServeMux()
		go func(portNum int) {
			portStr := strconv.Itoa(portNum)
			l, err := net.Listen("tcp", ":"+portStr)
			if err != nil {
				log.Info("Error while starting http on port ", portStr, " - ", err)
				return
			}
			// Note: http.Serve is a blocking call
			if err := http.Serve(l, mux); err != nil {
				log.Error("Server failed on port ", portStr, ": ", err)
			}
		}(i)
	}
	time.Sleep(1 * time.Second)
}

func main() {
	appHandler := testApplication{}
	appHandler.getCmdLineArgs()
	flag.Parse()
	appHandler.startHttpPort()
	go forever()
	select {}
}
