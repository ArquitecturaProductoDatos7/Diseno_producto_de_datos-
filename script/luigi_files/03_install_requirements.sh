#!/bin/bash

sudo apt update

sudo apt install python3-pip
sudo apt-get install postgresql-client

pip3 install -r Requirements.txt
pip3 install sklearn
pip3 install sklearn_pandas
pip3 install sqlalchemy


sudo apt install awscli

aws configure

