#!/bin/sh

cd /home/webspide/linux

echo stop
./House_WebSpideServerStop.sh
./House_WebSpideStop.sh

echo YiMuTian
./YiMuTian_WebSpideServer.sh
./YiMuTian_WebSpide.sh

echo CaiShiChang
./CaiShiChang_WebSpideServer.sh
./CaiShiChang_WebSpide.sh

echo House
nohup ./House_WebSpideServer.sh > /dev/null 2>&1 &
./House_WebSpide.sh
