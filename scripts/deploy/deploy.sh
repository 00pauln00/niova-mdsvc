rm -rf /work/ctlplane
mkdir -p /work/ctlplane/db
mkdir -p /work/ctlplane/ctl
mkdir -p /work/ctlplane/logs
rm -rf configs
./raft-config.sh
cp -r configs /work/ctlplane/
cp -r lib /work/ctlplane/
cp -r libexec /work/ctlplane/
cp run.sh /work/ctlplane/
pdsh -w 192.168.96.8[4-8] /work/ctlplane/run.sh 
