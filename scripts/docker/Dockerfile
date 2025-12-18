FROM ubuntu:noble

RUN apt-get update -y \
    && apt-get -y install uuid-runtime librocksdb8.9

WORKDIR controlplane

ENV NIOVA_LOCAL_CTL_SVC_DIR="/controlplane/configs"
ENV LD_LIBRARY_PATH="/lib:/controlplane/lib"
ENV NIOVA_INOTIFY_BASE_PATH="/controlplane/ctl-interface"
ENV NIOVA_APPLY_HANDLER_VERSION=0

ENTRYPOINT ["/controlplane/controlplane.sh"]
