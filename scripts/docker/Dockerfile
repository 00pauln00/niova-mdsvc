FROM ubuntu:noble

# Install dependencies
RUN apt-get update -y \
    && apt-get -y install uuid-runtime librocksdb8.9 \
    && apt-get clean

WORKDIR /controlplane

# Environment variables
ENV NIOVA_LOCAL_CTL_SVC_DIR="/controlplane/configs"
ENV LD_LIBRARY_PATH="/lib:/controlplane/lib"
ENV NIOVA_INOTIFY_BASE_PATH="/controlplane/ctl-interface"

# Copy control plane scripts
COPY controlplane.sh ./ 
COPY libexec ./libexec
COPY configs ./configs

# Make scripts executable
RUN chmod +x controlplane.sh

ENTRYPOINT ["/controlplane/controlplane.sh"]