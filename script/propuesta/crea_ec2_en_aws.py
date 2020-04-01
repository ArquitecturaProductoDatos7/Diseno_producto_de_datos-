#!/usr/bin/env python

""" Crea una instancia ec2 en AWS con las siguientes caracteristicas """

# ***************  Estos parametros se tienen que cambiar para cada usuario  ************************
#ID de la subnet de la VPC a la que le asociaremos la instancia  -subnet1-
SUBNET_ID = 'subnet-049fc7cc9e9466d60'
#El IDs del Security Group de la VPC donde va a estar la instancia
SECURITY_GROUP_ID = ['sg-09b7d6fd6a0daf19a']  
#Nombre de la llave privada con la que nos conectaremos a la instancia
KEYNAME_PAIR = 'key-dpa2020'
# ***************************************************************************************************

# Definimos otros parametros
EC2_TAG = 'ec2_dpa2020'
EC2_INSTANCE_TYPE = 't2.micro'  #tipo de la instancia (free)
EC2_AMI_TYPE = 'ami-07ebfd5b3428b6f4d' #Sistema operatvo (Ubuntu 18)

import boto3

# Creamos la instancia
ec2_client = boto3.resource('ec2')

instance = ec2_client.create_instances(
    InstanceType = EC2_INSTANCE_TYPE,
    ImageId = EC2_AMI_TYPE,
    MinCount = 1,  #minimun number of instances to launch
    MaxCount = 1,  #maximum number of instances to launch
    KeyName = KEYNAME_PAIR,
    NetworkInterfaces=[
        {
            'DeviceIndex': 0,
            'SubnetId': SUBNET_ID,
            'AssociatePublicIpAddress': True,
            'Groups': SECURITY_GROUP_ID
        },
    ]
   ) 
instance = instance[0]
instance_id = instance.id
print ("ID de la instancia: ", instance_id)

# Wait for the instance to enter the running state
instance.wait_until_running()

# Reload the instance attributes
instance.load()
print("DNS publica: ", instance.public_dns_name, "\nIPv4 publica", instance.public_ip_address)
