FROM ubuntu:noble

RUN apt-get update -y \
    && apt-get -y install uuid-runtime librocksdb8.9

WORKDIR controlplane

#Binaries(../libexec) and config file
ADD ./libexec ./libexec
#Config folder should contain raft config and gossipnodes
ADD ./configs ./configs
ADD ./logs ./logs
COPY lib/* /lib/

#Run pmdbServers and proxies
COPY ./run-controlplane.sh ./

ENV NIOVA_LOCAL_CTL_SVC_DIR="./configs"
ENV LD_LIBRARY_PATH = "/lib"
ENV NIOVA_INOTIFY_BASE_PATH="/controlplane/ctl-interface"

CMD ["./run-controlplane.sh"]
