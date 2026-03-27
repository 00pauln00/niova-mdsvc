DIR=/tmp/
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

install_all: compile pmdbserver proxyserver ncpcclient configapp testapp niova-ctl monitor ccManager install

install_only: compile pmdbserver proxyserver ncpcclient configapp testapp niova-ctl monitor ccManager install

compile:
	echo "Compiling controlPlane"
	mkdir -p libexec
	go mod tidy

pmdbserver:
	go build -o libexec/CTLPlane_pmdbServer ./controlplane/pmdbServer/

proxyserver:
	go build -o libexec/CTLPlane_proxy ./controlplane/proxy/

ncpcclient:
	go build -o libexec/ncpc ./controlplane/ncpc/

configapp:
	go build -o libexec/cfgApp ./controlplane/configApplication/

testapp:
	go build -o libexec/testApp ./controlplane/testApplication/

niova-ctl:
	go build -o libexec/niova-ctl ./controlplane/niova-ctl/

monitor: 
	go build -o libexec/cp-monitor ./controlplane/monitor/

ccManager: 
	go build -o libexec/cc-manager ./controlplane/containerConfigManager/

install:
	cp libexec/CTLPlane_pmdbServer ${DIR}/libexec/niova/CTLPlane_pmdbServer
	cp libexec/CTLPlane_proxy ${DIR}/libexec/niova/CTLPlane_proxy
	cp libexec/ncpc ${DIR}/libexec/niova/ncpc
	cp libexec/cfgApp ${DIR}/libexec/niova/cfgApp
	cp libexec/testApp ${DIR}/libexec/niova/testApp
	cp libexec/cp-monitor ${DIR}/libexec/niova/cp-monitor
	cp libexec/cc-manager ${DIR}/libexec/niova/cc-manager
	cp libexec/niova-ctl ${DIR}/libexec/niova/niova-ctl
	cp scripts/docker/Dockerfile ${DIR}
	cp scripts/docker/controlplane.sh ${DIR}
	cp scripts/docker/raft-config.sh ${DIR}
	cp scripts/docker/run-cpcontainer.sh ${DIR}	
	cp controlplane/authorizer/ctlauth.yaml ${DIR}
clean:
	rm -rf libexec
