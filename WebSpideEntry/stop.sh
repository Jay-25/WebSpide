#!/bin/sh

app=WebSpide.jar

mv $app $app.bak

ps -A -F | grep $app | grep java | awk '{system("kill -9 "$2)}' 
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1

mv $app.bak $app

echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1
#

app=WebSpideServer.jar

mv $app $app.bak

ps -A -F | grep $app | grep java | awk '{system("kill -9 "$2)}' 
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1
echo waiting...
sleep 1

mv $app.bak $app

