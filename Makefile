DIR=/tmp/
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

install_all: compile pmdbserver proxyserver ncpcclient configapp testapp niova-ctl install

install_only: compile pmdbserver proxyserver ncpcclient configapp testapp niova-ctl install

compile:
	echo "Compiling controlPlane"
	mkdir -p libexec
	go mod tidy

pmdbserver:
	go build -o libexec/CTLPlane_pmdbServer controlplane/pmdbServer/pmdbServer.go 

proxyserver:
	go build -o libexec/CTLPlane_proxy controlplane/proxy/proxy.go 

ncpcclient:
	go build -o libexec/ncpc controlplane/ncpc/ncpc.go 

configapp:
	go build -o libexec/cfgApp controlplane/configApplication/configApplication.go 

testapp:
	go build -o libexec/testApp controlplane/testApplication/testApplication.go

niova-ctl:
	cd controlplane/niova-ctl && go build -o ../../libexec/niova-ctl

install:
	cp libexec/CTLPlane_pmdbServer ${DIR}/libexec/niova/CTLPlane_pmdbServer
	cp libexec/CTLPlane_proxy ${DIR}/libexec/niova/CTLPlane_proxy
	cp libexec/ncpc ${DIR}/libexec/niova/ncpc
	cp libexec/cfgApp ${DIR}/libexec/niova/cfgApp
	cp libexec/testApp ${DIR}/libexec/niova/testApp
	cp libexec/niova-ctl ${DIR}/libexec/niova/niova-ctl
	cp scripts/docker/Dockerfile ${DIR}
	cp scripts/docker/controlplane.sh ${DIR}
	cp scripts/docker/raft-config.sh ${DIR}
	cp scripts/docker/run-cpcontainer.sh ${DIR}	
clean:
	rm -rf libexec
