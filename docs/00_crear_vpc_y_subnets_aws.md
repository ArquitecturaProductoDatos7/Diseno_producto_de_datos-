# Creación de VPC y Subnets en Amazon Web Services (AWS)

Esta es la documentación de cómo crear una VPC y dos Subnets requeridas para poder crear una RDB en AWS.

## Creación de VPC

* #### Una vez en la consola principal de AWS, se uso el buscador de servicios para encontrar el servicio de **VPC** (Virtual Private Cloud). Este nos llevo al VPC Dashboard.

 <center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/01_vpc_aws.png" width="600" height="350"/></center>

* #### Desde el VPC Dashboard, se usó el VPC Wizard haciendo click en el boton "Launch VPC Wizard"

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/02_vpc_aws.png" width="600" height="350"/></center>

* #### Paso 1: Se seleccionó "VPC with a Single Public Subnet"

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/03_vpc_aws.png" width="600" height="350"/></center>

* #### Paso 2: Se dejaron las IPs por default y se escribió el nombre de la VPC en el campo *VPC name:*, en mi caso la VPC se llama **vpc_dpa20**. Notemos que junto con la VPC se crea también una subnet pública.

Para la subnet pública se llenó el campo de *Availability Zone* seleccionando **us-east-1a** y el campo de *Subnet name*, en mi caso se llamó **subnet1_dpa20**. 

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/04_vpc_aws.png" width="600" height="350"/></center>

* #### Para crear la VPC se dió click en el boton "Create VCP". Obtuvimos un mensaje de que nuestra VPC fue creada exitosamente.

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/05_vpc_aws.png" width="600" height="350"/></center>
<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/06_vpc_aws.png" width="600" height="350"/></center>

La lista de VPC, en efecto mostró la **vpc_dpa20** creada.

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/07_vpc_aws.png" width="600" height="350"/></center>

También notamos que en la lista de subnets, aparecio la subnet pública **subnet1_dpa20**.

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/07a_vpc_aws.png" width="600" height="350"/></center>

## Creacion de Subnets

Para que la RDS pueda asociarse a muestra VPC, es necesario tener *al menos* dos subnets en *regiones diferentes (Availability Zones)*. Así que se creó una segunda subnet asociada a la VPC.

* #### Desde el VPC Dashboard, en el menú del lado derecho, se busco el servicio de Subnets

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/08_vpc_aws.png" width="600" height="350"/></center>

* #### Se dió click en el boton de "Crear subnet".

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/09_vpc_aws.png" width="600" height="350"/></center>

* #### Crear subnet: 
        - Se llenó el campo *Name tag* con una etiqueta para la subnet, en mi caso **subnet2_dpa20**
        - En el campo de *VPC\** se usó la VPC que se creo anteriormente **vpc_dpa20**. Esta aparece en el menú desplegable
        - Para el campo de *Availability Zone* se seleccióno una zona distinta a la seleccionada para la primera subnet. En mi caso fue **us-east-1c**.
        - En el campo de *IPv4 CIDR block\** se usó **10.0.128.0/24**. Si quieren saber porqué, pueden consultar [Adding IPv4 CIDR Blocks to a VPC](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Subnets.html#vpc-subnet-basics).
        
<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/10_vpc_aws.png" width="600" height="350"/></center>    

* #### Finalmente, se dió click en el boton "Crear Subnet" y se obtuvo un mensaje de confimación de que la creacion de la subnet fue exitosa.
        
Las nueva subnet **subnet2_dpa20** y la subnet creada con la VPC (**subnet1_dpa20**) aparecieron en la lista desplegada.

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/11_vpc_aws.png" width="600" height="350"/></center>


## Creamos un DB Subnet Group

Un DB Subnet Group es una colección de subredes (generalmente privadas --pero para nosotros públicas) que se crean para una VPC y que luego se asigna a la instancias de DB.

* #### Desde la consola principal de AWS, se uso el buscador de servicios para irse al servicio de **RDS**.

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/12_vpc_aws.png" width="600" height="350"/></center> 

* #### En el menú del lado derecho se dio click en la opción de "Subnet groups"

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/13_vpc_aws.png" width="600" height="350"/></center> 

* #### Después dimos click en el botón de "Create DB Subnet group"

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/14_vpc_aws.png" width="600" height="350"/></center> 

* #### En la primera parte, se llenaron los siguientes campos:
     - *Name* para el nombre del grupo, en mi caso **subnet_grp_dpa20**.
     - *Description*, puse por ejemplo "subnet para DB"
     - *VPC*, se eligió del menú desplegable la VPC con 2 subnets públicas, en mi caso **vpc_dpa20**.

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/15_vpc_aws.png" width="600" height="350"/></center>  

* #### En la segunda parte de la sección, dimos click en "Add all the subnets related to this VCP" y automaticamente añadió las subnets creadas anteriormente (**subnet1_dpa20**, **subnet2_dpa20**)

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/16_vpc_aws.png" width="600" height="350"/></center>  

* #### Dimos click en el boton "create" y en seguida nos listó la el grupo creado, **subnet_grp_dpa20**.

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/17_vpc_aws.png" width="600" height="350"/></center>  


## Creacion del Security Group

* #### Cuando se creó la VPC, se creo un Security group *default* que es el que se usará cuando se cree la instancia EC2. Para conocer el *Group ID* se seleccionó del menu de la derecha *Security Groups*

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/18_vpc_aws.png" width="600" height="350"/></center> 

* #### Se reconoció el security group porque esta asociado a la VPC que se creó. Lo que necesitamos identificar es el **Group ID**

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/19_vpc_aws.png" width="600" height="350"/></center> 

* #### También se editó el nombre para reconocerlaen el futuro

<center><img src="https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/AWS/VPC_y_Subnets/20_vpc_aws.png" width="600" height="350"/></center> 


Esto es es lo mínimo necesario para crear una RDS en Amazon Web Services.



Fuente: [Tutorial: Create an Amazon VPC for Use with a DB Instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_VPC.WorkingWithRDSInstanceinaVPC.html#USER_VPC.Subnets)
       
