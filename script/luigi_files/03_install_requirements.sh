#!/bin/bash

sudo apt update

sudo apt install python3-pip

pip3 install -r Requirements.txt

sudo apt-get install postgresql-client

sudo apt install awscli

aws configure

