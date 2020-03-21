# Creación de EC2 en Amazon Web Services (AWS).

* #### Una vez iniciada la sesión en la consola de AWS, en el buscador de servicios, se tecleó EC2 y se seleccionó.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/buscador.png)

* #### Posteriormente, se seleccionó “Launch instance”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/launch.png)

* #### Paso 1: Se escogió la AMI, en este caso “Ubuntu Server 18.04 LTS (HVM), SSD Volume Type”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/paso1.png)

* #### Paso 2: Se escogió el tipo de instancia, en este caso una t2.micro y se dio click en “Next:Configure Instance Details”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/paso2.png)

* #### Paso 3: Se seleccionó la VPC y la subnet en la que iba a estar la instancia, que en este caso solo tenemos una (subnet pública), ambas creadas en la sección anterior, y se dio click en “Next: Add Storage”

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/paso3.png)

* #### Paso 4: En esta ventana únicamente se dio click en “Next: Add Tags”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/paso4.png)

* #### Paso 5: En esta ventana únicamente se dio click en “Next: Configure Security Group”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/paso5.png)

* #### Paso 6: Se dio click en “Select an existing security group”, se seleccionó el security group que se creó en la sección anterior y se dió click en “Review and Launch”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/paso6.png)

* #### Paso 7: Se revisó que la configuración fuera correcta y se dió click en “Launch”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/paso7.png)

* #### Se seleccionó “Choose an existing key pair”, la llave y el cuadro en el que se indica que se tiene acceso a la llave seleccionada y posteriormente se dio click en “Launch Instances”

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/llave.png)

* #### En esta parte únicamente se dio click en “View Instances”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/status.png)

* #### Se empezó a “levantar” la instancia y cuando en el state apareció “running”, se selecccionó el botón de connect para iniciar la conexión.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/running.png)

* #### Se siguieron los siguientes pasos para iniciar la conexión desde la terminal. 

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/connect.png)
