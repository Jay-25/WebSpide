#!/bin/sh
export LANG=zh_CN.GBK

APP=../WebSpideServer.jar
APP_PARAS="-c ../ini/House_webspide.ini -stop"

mv $APP $APP.bak.jar
java -jar $APP.bak.jar $APP_PARAS
sleep 5
mv $APP.bak.jar $APP

