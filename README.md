
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

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/etl_op1.jpg)

####  Opción alternativa

Descargar los datos en formato CSV y guardarlos directamente como una base de datos en PostgreSQL. Siguiendo el mismo flujo, los guardaríamos en formato texto en un esquema *raw* y después de realizar las transformaciones indicadas anteriormente guardaríamos la base en el esquema *cleaned*.

Una vez que se tenga el esquema cleaned, se convertirá la base a un formato *Parquet* que se guardará en un S3.


![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/etl_op2.jpg)

### 2. Orchestration

Ocuparemos Luigi como orquestador de las tareas que vamos a ir realizando en el producto de datos, hasta el momento tenemos una tarea que se encarga de extraer los datos de la API del C5. Para realizar esta tarea tuvimos que utilizar "Pagination", para poder ir extrayendo subconjuntos de los registros, esto debido a que la API del C5 limita la extracción de los datos a 10000  y el total de la base de datos contiene más de 1,260,000 registros (tan sólo en la última actualización del 6 de febrero del 2020 fueron 1,267,760 registros).

En "Pagination" utilizaremos el método de "Page Number".

En la segunda fase el orquestador (Luigi) necesitará como requerimiento la tarea A (Extracción) y con esa extracción Luigi se encargará de hacer la conexión a AWS para que se guarde 

####  DAG

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/A.png)

### 3. Ética

La ética en nuestro producto de datos tiene dos vertienes principales. En primer lugar, para el desarrollo del producto de datos es necesario obtener información para poder analizarla e implementar un modelo para la toma de decisiones. En este caso, la extracción de información proporcionada por el C5 ya se encuentra acotada para porporcionar datos no sensibles, como por ejemplo: los nombres de las personas que realizaron la llamada o el teléfono que realizó la llamada, debido a que es una fuente de datos pública y se reservan ciertos datos, por lo que consideramos que la información que estamos utilizando no presenta un dilema ético para nosotros como científicos de datos, dado que cualquier persona tiene acceso a la misma información.

En segundo lugar, para la implementación del modelo que calcularan las probabilidades de que los incidentes viales reportados sean verdaderos, para posteriormente tener una lista priorizada de incidentes viales a atender con el fin hacer una correcta asignación de los recursos que tiene el C5. En esta fase, consideramos que si existen implicaciones éticas importantes porque nuestro modelo de clasificación etiqueta cada incidencia vial nueva en "verdadero"-(alta probabilidad de ser un caso verdadero) dependendiendo de la probabilidad que arroje ese indicidente reportado. Derivado de este resultado, la ética es un factor a considerar en nuestro proyecto porque si nuestro modelo llegara a etiquetar un incidente como "no verdadero" o con baja probabilidad, pero en la realidad si es "verdadero" generaría que el recurso se enviara a otro destino y podría incurrir incluso en alguna pérdida humana.
Es por eso que al momento de desarrollar nuestro modelo nos enfocaremos más en la métrica de "Recall" para monitorear del total de incidentes viales que realmente fueron verdaderos, cuantos de esos nuestro modelo clasificó como verdadero.

De igual manera, es importante mencionar que el producto de datos sólo es una herramienta adicional que será proporcionada al tomador de decisiones del C5 ( por ejemplo un supervisor), el cual se apoyará del producto de datos con la lista de probabilidades priorizadas, pero éste último es el que tomará la decisión si mandar el recurso o no ( o el tipo de recurso) al área solicitada.

## Referencias

C5:
