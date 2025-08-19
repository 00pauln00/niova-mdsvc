package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"

	cpClient "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/client"

	"github.com/google/uuid"
)

type s3Config struct {
	URL  string `yaml:"url"`
	Opts string `yaml:"opts"`
	Auth string `yaml:"auth"`
}

type NisdConfig struct {
	Name   string `yaml:"name"`
	Nisd   string `yaml:"uuid"`
	Client uint16 `yaml:"client_port"`
	Peer   uint16 `yaml:"peer_port"`
}

type ContainerConfig struct {
	S3Config   s3Config     `yaml:"s3_config"`
	NisdConfig []NisdConfig `yaml:"nisd_config"`
}

func populateNisd(nisd map[string][]byte, UniqID string) NisdConfig {
	cp, _ := strconv.ParseUint(string(nisd[fmt.Sprintf("/n/%s/conf_cp", UniqID)]), 10, 16)
	pp, _ := strconv.ParseUint(string(nisd[fmt.Sprintf("/n/%s/conf_pp", UniqID)]), 10, 16)
	return NisdConfig{
		Name:   string(nisd[fmt.Sprintf("/n/%s/conf_d", UniqID)]),
		Nisd:   UniqID,
		Client: uint16(cp),
		Peer:   uint16(pp),
	}
}

// LoadOrCreateConfig loads config from file if present, otherwise creates a new one.
func LoadOrCreateConfig(setupConfig string, cc *ContainerConfig) error {
	if err := os.MkdirAll(filepath.Dir(setupConfig), os.ModePerm); err != nil {
		return err
	}

	if _, err := os.Stat(setupConfig); err == nil {
		data, err := os.ReadFile(setupConfig)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(data, cc); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	return nil
}

func main() {

	raftID := flag.String("r", "", "pass the raft uuid")
	configPath := flag.String("c", "./gossipNodes", "pass the gossip config path")
	setupConfig := flag.String("sc", "./config.yaml", "pass the gossip config path")
	deviceList := flag.String("d", "", "pass the nisd device id")
	flag.Parse()
	deviceIDs := strings.Split(*deviceList, ",")
	log.Infof("starting config app - raft: %s, config: %s, device id: %s", *raftID, *configPath, *deviceList)

	c := cpClient.InitCliCFuncs(uuid.New().String(), *raftID, *configPath)
	var cc ContainerConfig
	cc.NisdConfig = make([]NisdConfig, 0)
	err := LoadOrCreateConfig(*setupConfig, &cc)
	if err != nil {
		log.Error("failed to load or create config file: ", err)
		os.Exit(-1)
	}
	for _, deviceID := range deviceIDs {
		uid, err := c.GetDeviceUUID(deviceID)
		if err != nil {
			log.Error("failed to get device uuid", err)
			os.Exit(-1)
		}
		log.Info("device uuid: ", string(uid))
		nisd, err := c.GetNisdDetails(string(uid))
		if err != nil {
			log.Error("failed to get nisd details:", err)
		}
		cc.NisdConfig = append(cc.NisdConfig, populateNisd(nisd, string(uid)))
	}

	log.Info("config struct: ", cc)
	configY, err := yaml.Marshal(cc)
	if err != nil {
		log.Error("Error marshaling YAML:", err)
		os.Exit(-1)
	}
	err = os.WriteFile(filepath.Dir(*setupConfig)+"/config-gen.yaml", configY, 0644)
	if err != nil {
		log.Error("Error writing YAML file:", err)
		os.Exit(-1)
	}
}
