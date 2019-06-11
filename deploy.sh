#!/bin/bash
#echo "Updating Packages"
#sudo apt update -y
echo "Install pip3"
sudo apt install -y python3-pip
echo "Install fuse"
sudo pip3 install fusepy requests
echo "Clone repository"
git clone https://github.com/pmilano1/nas_livemount.git
echo "user_allow_other" >> /etc/fuse.conf
mkdir nas_livemount/tmp
chown -R vagrant:vagrant .
echo 'Install CockroachDB'
wget -qO- https://binaries.cockroachdb.com/cockroach-v19.1.1.linux-amd64.tgz | tar  xvz
cp -i cockroach-v19.1.1.linux-amd64/cockroach /usr/local/bin
echo 'Start CockroachDB'
cockroach start --insecure --listen-addr=localhost &&
