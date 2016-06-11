#!/bin/sh

app=$1

mv $app $app.bak

ps -A -F | grep $app | grep java | awk '{system("kill -9 "$2)}' 
echo "waiting(5)..."
sleep 1
echo "waiting(4)..."
sleep 1
echo "waiting(3)..."
sleep 1
echo "waiting(2)..."
sleep 1
echo "waiting(1)..."
sleep 1

mv $app.bak $app



