#!/bin/sh

while [ TRUE ]
do
    sleep 1
    clear

    echo "USER       PID %CPU %MEM    VSZ   RSS  COMMAND"
    ps aux | grep WebSpide.*.sh$ | sed -e "s/\?.*\///g"
    echo "----------"
    echo "USER       PID %CPU %MEM    VSZ   RSS  COMMAND"
    ps aux | grep java | grep jar |  sed -e "s/\?.*-jar//g"
done

echo "USER       PID %CPU %MEM    VSZ   RSS  COMMAND"
ps aux | grep WebSpide.*.sh$ | sed -e "s/\?.*\///g"
echo "----------"
echo "USER       PID %CPU %MEM    VSZ   RSS  COMMAND"
ps aux | grep java | grep jar |  sed -e "s/\?.*-jar//g"

