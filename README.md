
## Definición de proyecto a realizar

La base de datos elegida para el trabajo es la de [Incidentes viales](https://datos.cdmx.gob.mx/explore/dataset/incidentes-viales-c5/table/?disjunctive.incidente_c4) reportados por el Centro de Comando, Control, Cómputo, Comunicaciones y Contacto Ciudadano de la Ciudad de México (C5) desde 2014 y actualizado mensualmente.

Los incidentes de emergencia se reportan al C5, principalmente, por medio de llamadas. Después de recibir la solicitud, se envía una unidad de emergencia al lugar del incidente. Una vez en el lugar del incidente, se confirma la emergencia reportada. Sin embargo, en ocasiones las solicitudes son no procedentes o falsas (bromas, niños jugando, etc.), lo cual implica un desvío de recursos limitados, postergando la atención de otras solicitudes que requieren la ayuda.

Dado lo anterior, el proyecto tiene como objetivo identificar los incidentes de emergencias "reales" para una mejor asignación de los servicios de emergencia de la Ciudad de México (grúas, patrullas, ambulancias, médicos, etc.). 

Para esto, desarrollaremos un modelo para clasificar cada incidente como "verdadero". Es decir, que el código de cierre sea *Afirmativo*, pues este identifica cuando una unidad de emergencias fue despachada, llego al lugar de los hechos y confirmo la emergencia reportada.


## Diseño del producto de datos (mockup)

De un análisis inicial de la base de datos, identificamos que se reciben varias solicitudes al mismo tiempo, por lo que es posible que los recursos disponibles no cubran la demanda de servicio. El producto de datos generará una lista priorizada de incidentes a atender, ordenada de mayor a menor según su  probabilidad de *ser una solicitud Afirmativa*.  

La utilidad en este producto de datos recide en poder priorizar con antelación cada uno de los incidentes antes de ser atendido, para poder eficientar la operación en temas de seguridad pública, urgencias médicas, protección civil, movilidad y servicios a la comunidad en la capital del país.


### Entregables

Un **Dashboard** que presente la información de la lista de manera dinámica, actualizando las solicitudes que ya fueron atendidas y calculando de modo automático el tiempo de respuesta para realizar estadísticas futuras.

![alt text](https://github.com/brunocgf/Productos_de_Datos_2020/blob/master/imagenes/mockup1.png)

### 1. ETL

####  Primera opción

Los datos de incidentes viales estan publicados de 2014 a enero 2020 y son actualizados mensualmente.

La ingestión de los datos se hara una vez para obtener los datos disponibles (2014 a la fecha de ingestión) y luego se hará mensualmente para actualizar los datos.

La página de datos de la CDMX provee una API para hacer solicitudes de descarga, la cual utilizaremos para extraer los datos.

Los datos extraídos están en formato JSON, que es el que devuelve la API, y se almacenarán en un objeto S3 en AWS (esquema *raw*).

Una vez teniendo el esquema anterior, lo siguiente a realizar sería convertir los datos al formato **Parquet** dado que el número de variables es pequeño (17 columnas), no se tienen variables numéricas, solo de fecha y geográficas.

Posteriormente se realizarían transformaciones para:
     
     - Eliminar variables que no se usarán, por ejemplo la variable de *geopoint*
     - Eliminar los caracteres especiales de los datos y convertir todo a minúsculas

Con estos cambios, guardamos la información en S3 bajo el esquema *cleaned*.

![alt text](https://github.com/brunocgf/Productos_de_Datos_2020/blob/master/imagenes/etl_op1.png)

####  Opción alternativa

Descargar los datos en formato CSV y guardarlos directamente como una base de datos en PostgreSQL. Siguiendo el mismo flujo, los guardaríamos en formato texto en un esquema *raw* y después de realizar las transformaciones indicadas anteriormente guardaríamos la base en el esquema *cleaned*.

Una vez que se tenga el esquema cleaned, se convertirá la base a un formato *Parquet* que se guardará en un S3.

![alt text](https://github.com/brunocgf/Productos_de_Datos_2020/blob/master/imagenes/etl_op2.png)

## Referencias

C5:
