#!/bin/bash

# Este script copia los siguientes archivos a la ec2 en AWS
#     Llave privada
#     Requirements.txt
#     luigi_files/*   
# Se requiere:
#     PATH_TO_KEY  Ruta donde se encuentra la llave, ~/path/to/file/key.pem
#     PUBLIC_DNS   El DNS publico de la ec2 que se esta utilizando


PATH_TO_KEY=$1  
PUBLIC_DNS=$2
BASE_PATH=$PWD

aux=${PUBLIC_DNS%%.compute-1.amazonaws.com}
aux=${aux#ec2-}
aux=${aux//-/.}

scp -i ${PATH_TO_KEY} ${PATH_TO_KEY} ubuntu@$aux:/home/ubuntu/.ssh

scp -i ${PATH_TO_KEY} ${BASE_PATH}/Requirements.txt ubuntu@$aux:/home/ubuntu

scp -i ${PATH_TO_KEY} ${BASE_PATH}/luigi_files/* ubuntu@$aux:/home/ubuntu
        

