#!/bin/sh

cd /home/webspide

echo "YiMuTian"
./YiMuTian_WebSpideServer.sh > /dev/null 2>&1
./YiMuTian_WebSpide.sh > /dev/null 2>&1

echo "CaiShiChang"
./CaiShiChang_WebSpideServer.sh > /dev/null 2>&1
./CaiShiChang_WebSpide.sh > /dev/null 2>&1

echo "House"
./House_WebSpideServer.sh > /dev/null 2>&1
./House_WebSpide.sh > /dev/null 2>&1


