#Provide start of the port range as command line arg
#I.e sudo ./run-cpcontainer.sh 5000
if [ "$2" = "init" ]; then
        rm -rf *.raftdb configs ctl-interface logs
fi

docker build -t controlplane .
curdir=$(pwd)
docker run -it -v "${curdir}:/controlplane" --network=host controlplane $1 $2 $3
