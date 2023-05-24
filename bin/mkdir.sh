#!/bin/bash

#mkdir -p $1
#chown friedel:des_dm $1
#chmod -R g+w $1
root=$1
shift 1
for path in "$@"
do
    root+="/${path}"
    mkdir -p $root
    #echo "mkdir -p ${root}"
    chown ${USER}:des_dm $root
    chmod g+w $root
done
