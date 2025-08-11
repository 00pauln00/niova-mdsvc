package main

import (
	"flag"

	log "github.com/sirupsen/logrus"

	cpClient "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/client"
	uuid "github.com/satori/go.uuid"
)

func main() {

	raftID := flag.String("r", "", "pass the raft uuid")
	configPath := flag.String("c", "", "pass the gossip config path")
	deviceID := flag.String("d", "", "pass the nisd device id")
	flag.Parse()
	log.Infof("starting config app - raft: %s, config: %s, device id: %s", *raftID, *configPath, *deviceID)
	c := cpClient.InitCliCFuncs(uuid.NewV4().String(), *raftID, *configPath)
	err := c.GetNisdDetails(*deviceID)
	if err != nil {
		log.Error(err)
	}
}
