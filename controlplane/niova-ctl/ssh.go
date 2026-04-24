package main

import (
	"bufio"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"

	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
)

type SSHClient struct {
	client *ssh.Client
}

func NewSSHClient(hosts []string) (*SSHClient, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get current user: %v", err)
	}

	keyPath := filepath.Join(usr.HomeDir, ".ssh", "id_rsa")
	key, err := os.ReadFile(keyPath)
	if err != nil {
		keyPath = filepath.Join(usr.HomeDir, ".ssh", "id_ed25519")
		key, err = os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read SSH key from ~/.ssh/id_rsa or ~/.ssh/id_ed25519: %v", err)
		}
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %v", err)
	}

	config := &ssh.ClientConfig{
		User: usr.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	var lastErr error
	for _, host := range hosts {
		if !strings.Contains(host, ":") {
			host = host + ":22"
		}

		client, err := ssh.Dial("tcp", host, config)
		if err == nil {
			return &SSHClient{client: client}, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed to connect to any host: %v", lastErr)
}

func (s *SSHClient) Close() error {
	return s.client.Close()
}

func (s *SSHClient) RunCommand(cmd string) (string, error) {
	session, err := s.client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	output, err := session.CombinedOutput(cmd)
	return string(output), err
}

func (s *SSHClient) GetDevices() ([]ctlplfl.Device, error) {
	// deviceMap is keyed by block device name (e.g. "sda").
	// This lets us deduplicate multiple by-id symlinks that point to the
	// same disk and merge lsblk metadata into the correct entry.
	deviceMap := make(map[string]*ctlplfl.Device)

	byIdOutput, err := s.RunCommand("ls -la /dev/disk/by-id/ 2>/dev/null | grep -v '^total' | grep -v '^d' | awk '{print $9, $11}'")
	if err == nil && byIdOutput != "" {
		for _, dev := range parseByIdDevices(byIdOutput) {
			// DevicePath is "/dev/sda"; strip the prefix to get the block name.
			blockName := filepath.Base(dev.DevicePath)
			if _, exists := deviceMap[blockName]; !exists {
				// Keep only the first by-id entry per physical disk.
				// All symlinks for the same disk share the same DevicePath;
				// we want one representative with a unique hardware-based Name.
				d := dev
				deviceMap[blockName] = &d
			}
		}
	}

	lsblkOutput, err := s.RunCommand("lsblk -d -n -o ID,NAME,SIZE,SERIAL,TYPE | grep 'disk'")
	if err != nil {
		return nil, fmt.Errorf("failed to get device list: %v", err)
	}

	for _, lsblkDev := range parseLsblkDevices(lsblkOutput) {
		if existing, exists := deviceMap[lsblkDev.Name]; exists {
			// Enrich the by-id entry with size and serial from lsblk.
			existing.Size = lsblkDev.Size
			if existing.SerialNumber == "" {
				existing.SerialNumber = lsblkDev.SerialNumber
			}
		} else {
			// No by-id entry for this disk — use lsblk data as-is.
			log.Info("Add device to list (no by-id entry): ", lsblkDev)
			d := lsblkDev
			deviceMap[lsblkDev.Name] = &d
		}
	}

	result := make([]ctlplfl.Device, 0, len(deviceMap))
	for _, dev := range deviceMap {
		result = append(result, *dev)
	}
	return result, nil
}

func parseByIdDevices(output string) []ctlplfl.Device {
	devices := make([]Device, 0)
	lines := strings.Split(strings.TrimSpace(output), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) >= 2 {
			id := parts[0]
			target := parts[1]

			// Skip partition devices (contain "-part" in the ID)
			if strings.Contains(id, "-part") {
				continue
			}

			if strings.Contains(target, "../") {
				deviceName := strings.TrimPrefix(filepath.Base(target), "../")

				// Additional check: skip if device name contains 'p' followed by digits (nvme partitions)
				// or ends with digits (sda1, sdb2, etc.)
				if regexp.MustCompile(`p\d+$|[a-z]\d+$`).MatchString(deviceName) {
					continue
				}

				devices = append(devices, Device{
					ID:         id,
					Name:       id, // by-id name is unique per hardware across all hypervisors
					DevicePath: "/dev/" + deviceName,
				})
			}
		}
	}

	return devices
}

// parseSizeToBytes converts human-readable size strings (like "1T", "500G", "1.2G") to bytes
// Examples:
//
//	"1T" -> 1099511627776 (1 * 1024^4)
//	"500G" -> 536870912000 (500 * 1024^3)
//	"1.5G" -> 1610612736 (1.5 * 1024^3)
//	"2048M" -> 2147483648 (2048 * 1024^2)
//	"1024K" -> 1048576 (1024 * 1024)
//	"12345" -> 12345 (raw bytes)
func parseSizeToBytes(sizeStr string) int64 {
	if sizeStr == "" {
		return 0
	}

	// Remove any whitespace
	sizeStr = strings.TrimSpace(sizeStr)
	if sizeStr == "" {
		return 0
	}

	// Extract numeric part and unit
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGTPE]?)$`)
	matches := re.FindStringSubmatch(strings.ToUpper(sizeStr))
	if len(matches) < 3 {
		// Try to parse as raw number (bytes)
		if bytes, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
			return bytes
		}
		return 0
	}

	// Parse the numeric part
	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0
	}

	// Apply multiplier based on unit
	unit := matches[2]
	var multiplier int64 = 1

	switch unit {
	case "K":
		multiplier = 1024
	case "M":
		multiplier = 1024 * 1024
	case "G":
		multiplier = 1024 * 1024 * 1024
	case "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "P":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024
	case "E":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024 * 1024
	case "":
		multiplier = 1 // No unit means bytes
	}

	return int64(value * float64(multiplier))
}

func parseLsblkDevices(output string) []ctlplfl.Device {
	devices := make([]Device, 0)
	scanner := bufio.NewScanner(strings.NewReader(output))

	// Regex to handle ID NAME SIZE SERIAL TYPE format.
	// ID may be empty (lsblk pads with spaces), NAME and SIZE are always present,
	// SERIAL is optional, TYPE is "disk".
	re := regexp.MustCompile(`^(\S*)\s+(\S+)\s+(\S+)\s+(\S*)\s+disk`)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		matches := re.FindStringSubmatch(line)
		if len(matches) >= 5 {
			id := matches[1]
			name := matches[2]
			sizeStr := matches[3]
			serialNum := matches[4]

			if id == "" {
				id = name
			}

			// Convert size string to bytes
			sizeBytes := parseSizeToBytes(sizeStr)
			log.Infof("Device %s: size string '%s' parsed to %d bytes", name, sizeStr, sizeBytes)

			devices = append(devices, Device{
				ID:           id,
				Name:         name,
				DevicePath:   "/dev/" + name,
				Size:         sizeBytes,
				SerialNumber: serialNum,
			})
		}
	}

	return devices
}
