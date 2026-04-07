#Provide start of the port range as command line arg
#I.e sudo ./run-cpcontainer.sh 5000
if [ "$2" = "init" ]; then
        rm -rf rocksdb configs ctl-interface logs
fi

# Set AUTH_ENABLED to false only if explicitly requested
AUTH_OPT=""
if [ "$3" = "false" ]; then
    AUTH_OPT="-e AUTH_ENABLED=false"
fi

docker build -t controlplane .
curdir=$(pwd)
docker run -it ${AUTH_OPT} -v "${curdir}:/controlplane" --network=host controlplane $1 $2
