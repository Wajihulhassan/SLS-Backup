#!/usr/bin/env bash

if [ "$#" -ne 1 ] ; then
    echo "Illegal number of parameters- use ON or OFF as parameter"
    exit 1
fi

if [ "$1" == "OFF" ]; then
	sed -i -e 's/export HADOOP_ROOT_LOGGER="ALL,console"/export HADOOP_ROOT_LOGGER="INFO,console"/g' ~/hadoop-2.7.0/etc/hadoop/hadoop-env.sh
	echo "OFF debugging statments"
fi

if [ "$1" == "ON" ]; then
   sed -i -e 's/export HADOOP_ROOT_LOGGER="INFO,console"/export HADOOP_ROOT_LOGGER="ALL,console"/g' ~/hadoop-2.7.0/etc/hadoop/hadoop-env.sh
   echo "ON debugging statments"
 fi

