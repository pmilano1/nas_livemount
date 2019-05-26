#!/bin/bash
echo "Updating Packages"
sudo apt update -y
echo "Install pip3"
sudo apt install -y python3-pip
echo "Install fuse"
sudo pip3 install fuse requests
echo "Clone repository"
git clone https://github.com/pmilano1/nas_livemount.git