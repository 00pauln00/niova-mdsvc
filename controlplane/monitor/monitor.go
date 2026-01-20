package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	totalSpace = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nisd_total_space_bytes",
			Help: "Total space per NID",
		},
		[]string{"nisd"},
	)

	availableSpace = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nisd_available_space_bytes",
			Help: "Available space per NID",
		},
		[]string{"nisd"},
	)

	availableRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nisd_available_space_ratio",
			Help: "Available space ratio per NID",
		},
		[]string{"nisd"},
	)
)

var (
	re = regexp.MustCompile(`n_cfg/([a-f0-9\-]+)/([a-z]+)$`)
)

func runLDB(dbPath string) error {
	cmd := exec.Command(
		"ldb",
		"dump",
		"--db="+dbPath,
		"--column_family=PMDBTS_CF",
		"--hex",
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("ldb failed: %v: %s", err, stderr.String())
	}

	type entry struct {
		ts uint64
		as uint64
	}

	state := make(map[string]*entry)
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "==>", 2)
		if len(parts) != 2 {
			continue
		}

		keyHex := strings.TrimPrefix(strings.TrimSpace(parts[0]), "0x")
		valHex := strings.TrimPrefix(strings.TrimSpace(parts[1]), "0x")

		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			log.Println("error decoding key:", err)
			continue
		}
		valBytes, err := hex.DecodeString(valHex)
		if err != nil {
			log.Println("error decoding value:", err)
			continue
		}

		key := string(keyBytes)
		valStr := strings.TrimSpace(string(valBytes))
		m := re.FindStringSubmatch(key)
		if m == nil {
			continue
		}

		nid := m[1]
		field := m[2]

		val, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			continue
		}

		e, ok := state[nid]
		if !ok {
			e = &entry{}
			state[nid] = e
		}

		switch field {
		case "ts":
			e.ts = val
			totalSpace.WithLabelValues(nid).Set(float64(val))
		case "as":
			e.as = val
			availableSpace.WithLabelValues(nid).Set(float64(val))
		}
	}

	for nid, e := range state {
		if e.ts > 0 {
			availableRatio.WithLabelValues(nid).
				Set(float64(e.as) / float64(e.ts))
		}
	}

	return scanner.Err()
}

func main() {
	prometheus.MustRegister(totalSpace, availableSpace, availableRatio)

	// dbPath as input parameter
	dbPath := flag.String("db", "", "Path to the RocksDB database")
	flag.Parse()

	if *dbPath == "" {
		log.Fatal("db path must be provided with -db flag")
	}

	// Run once immediately, then every 60s
	go func() {
		for {
			if err := runLDB(*dbPath); err != nil {
				log.Println("failed to run ldb:", err)
			}
			time.Sleep(60 * time.Second)
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":9700", nil))
}
