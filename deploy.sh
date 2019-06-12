#!/bin/bash
#echo "Updating Packages"
sudo apt update -y
echo "Install pip3 and db libs"
sudo apt install -y python3-pip libpq-dev
echo "Install fuse"
sudo pip3 install fusepy requests psycopg2
echo "Clone repository"
git clone https://github.com/pmilano1/nas_livemount.git
echo "user_allow_other" >> /etc/fuse.conf
mkdir nas_livemount/tmp
chown -R vagrant:vagrant .
cd nas_livemount
git checkout cachedb
