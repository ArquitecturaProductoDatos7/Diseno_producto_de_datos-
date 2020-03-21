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

* #### Y llegar a lo siguiente:

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/conexion.png)

#### Una vez que se observó que la conexión fue exitosa, se tecleó “exit” para salir de ella y copiar la llave, con la que se “levantó” la instancia, desde local hacia la carpeta .ssh de la instancia, ejecutando el siguiente comando:

`scp -i /Users/maggiemusa/.ssh/key-apd.pem /Users/maggiemusa/.ssh/key-apd.pem ubuntu@54.221.60.22:/home/ubuntu/.ssh`


#### Posteriormente, nuevamente desde local, se copiaron los scripts de python para ejecutar el ETL de los datos mediante luigi, ejecutando lo siguiente:

`scp -i key-apd.pem /Users/maggiemusa/Documents/Productos_datos/etl_pipeline.py ubuntu@54.221.60.22:/home/ubuntu`

`scp -i key-apd.pem /Users/maggiemusa/Documents/Productos_datos/extrae.py ubuntu@54.221.60.22:/home/ubuntu`

#### Nos conectamos a la instancia y corroboramos que se hayan copiado los archivos descritos:

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/archivos_ec2.png)

#### Desde la instancia, para poder correr los scripts, se instaló python y los siguientes paquetes y versiones con el comando `pip install`:

- `boto3==1.12.12`
- `botocore==1.15.12`
- `luigi==2.8.12`
- `numpy==1.16.6`
- `pandas==0.24.2`
- `psycopg2-binary==2.8.4`
- `python-daemon==2.2.4`
- `python-dateutil==2.8.1`
- `requests==2.23.0

**Nota: si se tienen problemas para instalar luigi probar con:** `export PATH =”~/.local/bin:$PATH”`

#### Así también, desde la instancia, se configuró la línea de comando de AWS, con lo siguiente:

`pip install awscli –upgrade`

#### y luego:

`aws configure`

#### En el que se capturó la aws_access_key_id, la aws_secret_access_key y la region, estas credenciales puedes obtenerlas iniciando sesión en la cuenta de AWS educate, dando click en “Account Details” y luego en “Show”.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/credentials_educate.png)

#### Y así se crea en la carpeta .aws de la instancia el archivo credentials.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/EC2/credentials_ec2.png)

#### Es importante señalar que dentro de las credenciales se encuentra el aws_session_token, mismo que no se pide cuando se configura aws, sin embargo, se debe agregar, modificando el archivo credentials con el comando `nano`.

#### No se omite señalar, que estas credenciales cambian cada que se cierra sesión por lo que éstas deben ser actualizadas, utilizando el comando `aws configure`.

#### Para poder correr luigi desde la instancia, al principio del script etl_pipeline.py se ingresó lo siguiente:

  - `import sys`
  - `reload(sys)`
  - `sys.setdefaultencoding('utf-8')`


