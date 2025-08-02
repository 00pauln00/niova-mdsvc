# Controlplane Container Setup

This project provides a streamlined way to set up and run a Controlplane services. The setup launches a single container that internally spawns:

- **5 Controlplane PMDB servers**
- **1 Proxy server**

The container shares:
- The **CTL interface** with the host for external control commands.
- The **log files** to the host for easy debugging and monitoring.
- The **config files** with the host for external services to connect to controlplane services.

---

## Installation and Setup

Follow these steps to prepare and run the environment, all paths mentioned are from the root of the git repo.

### 1. Install submodules

```bash
1. Install niova-core, set the prefix path to <INSTALL_DIR>
2. Install niova-raft
3. Install niova-pumicedb
```

### 3. Install `controlplane`

```bash
cd controlplane
make -e DIR=<INSTALL_DIR>
```

---

## Execute controlplane container

Execute the container script with a start port, which serves as the base for service port allocation.

```bash
cd <INSTALL_DIR>
./run-cpcontainer.sh <START_PORT>
```



