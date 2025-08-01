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

Follow these steps to prepare and run the environment:

### 1. Install `niova-core`

### 2. Install `niova-block`

```bash
./prepare --prefix=<INSTALL_DIR> --with-niova=<INSTALL_DIR>
```

### 3. Install `controlplane`

```bash
cd s3
make -e DIR=<INSTALL_DIR>
```

---

## Execute controlplane container

Execute the container script with a start port, which serves as the base for service port allocation.

```bash
cd <INSTALL_DIR>
./run-cpcontainer.sh <START_PORT>
```



