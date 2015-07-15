#!/usr/bin/env bash

set -x
cd ~/wajih-hadoop2.7.0 

git status

git pull

mvn clean  install -DskipTests

