#Provide start of the port range as command line arg
#I.e sudo ./run-cpcontainer.sh 5000
rm -rf *.raftdb configs ctl-interface logs
docker build -t controlplane .
curdir=$(pwd)
docker run -it -v "${curdir}:/controlplane" --network=host controlplane $1
