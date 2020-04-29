
## Definición de proyecto a realizar

La base de datos elegida para el trabajo es la de [Incidentes viales](https://datos.cdmx.gob.mx/explore/dataset/incidentes-viales-c5/table/?disjunctive.incidente_c4) reportados por el Centro de Comando, Control, Cómputo, Comunicaciones y Contacto Ciudadano de la Ciudad de México (C5) desde 2014 y actualizado mensualmente.

Los incidentes de emergencia se reportan al C5, principalmente, por medio de llamadas. Después de recibir la solicitud, se envía una unidad de emergencia al lugar del incidente. Una vez en el lugar del incidente, se confirma la emergencia reportada. Sin embargo, en ocasiones las solicitudes son no procedentes o falsas (bromas, niños jugando, etc.), lo cual implica un desvío de recursos limitados, postergando la atención de otras solicitudes que requieren la ayuda.

### Pregunta de investigación

Dado lo anterior, el proyecto tiene como objetivo identificar los incidentes de emergencias "reales" para una mejor asignación de los servicios de emergencia de la Ciudad de México (grúas, patrullas, ambulancias, médicos, etc.). 

Para esto, desarrollaremos un modelo para clasificar cada incidente como "verdadero". Es decir, que el código de cierre sea *Afirmativo*, pues este identifica cuando una unidad de emergencias fue despachada, llego al lugar de los hechos y confirmó la emergencia reportada.

## Diseño del producto de datos (mockup)

De un análisis inicial de la base de datos, identificamos que se reciben varias solicitudes al mismo tiempo, por lo que es posible que los recursos disponibles no cubran la demanda de servicio. El producto de datos generará una lista priorizada de incidentes a atender, ordenada de mayor a menor según su  probabilidad de *ser una solicitud Afirmativa*.  

La utilidad en este producto de datos recide en poder priorizar con antelación cada uno de los incidentes antes de ser atendido, para poder eficientar la operación en temas de seguridad pública, urgencias médicas, protección civil, movilidad y servicios a la comunidad en la capital del país.

### Entregables

Un **Dashboard** que presente la información de la lista priorizada en forma dinámica, actualizando las solicitudes que ya fueron atendidas y calculando de modo automático el tiempo de respuesta para realizar estadísticas futuras.<br>  <hr> 

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/mockup.png)

### 1. ETL

* Los datos de incidentes viales estan publicados de 2014 a enero 2020 y son actualizados mensualmente.<br>
* La ingestión de los datos se hara una vez para obtener los datos disponibles (2014 a la fecha de ingestión) y luego se hará mensualmente para actualizar los datos.<br>
* La página de datos de la CDMX provee una API para hacer solicitudes de descarga, la cual utilizaremos para extraer los datos.<br>
* Los datos extraídos están en formato JSON, que es el que devuelve la API, y se almacenarán en una RDS de AWS (esquema *raw*).<br>
* Una vez con la ingesta de la base de datos en el esquema *raw* se realizarían transformaciones para:<br> 
     - Eliminar variables que no se usarán, por ejemplo la variable de *geopoint*<br>
     - Eliminar los caracteres especiales de los datos y convertir todo a minúsculas<br>
     - Se eliminarán acentos.<br>
* Una vez teniendo el esquema anterior se realizará la limpieza de la base de datos y se pasará al esquema *cleaned* en la RDS. En esta parte, de acuerdo al EDA (Exploratory Data Analysis) las variables que se quedarón para el modelo son las siguientes: hora_creacion, delegacion_inicio, dia_semana,tipo_entrada, mes, latitud,longitud,ano,incidente_c4,codigo_cierre. 
Se puede encontrar el diccionario de estas variables en la siguiente liga [EDA](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/tree/master/EDA).<br> <hr> 

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/ETL.png)

### 2. Orchestration

Ocuparemos Luigi como orquestador de las tareas que vamos a ir realizando en el producto de datos, hasta el momento tenemos las siguientes tareas:<br>
* 1) **EtlPipeline** - que se encarga de inicializar las tareas de *ObtieneRDSHost*, *ExtraeInfoPrimeraVez* y *InsertaMetadatosCLEANED* en ese orden.<br>
* 2) **ObtieneRDSHost** - con la cual se obtiene el endpoint(host) de la RDS creada para poder hacer la conexión posteriormente y requiere que la base de datos en postgres ya este creada.<br>
* 3) **CreaInstanciaRDS** - con esta tarea se crea la base de datos en postgres cuando se obtiene un "Subnet Group".<br>
* 4) **ExtraeInfoPrimeraVez** - Aqui luigi lo que hace es extraer toda la informacion desde la API del C5 (desde el 1-Ene-2014), el rango superior de la extracción depende de la fecha en que se corre el pipeline. Si se corre antes del 15 de mes se extrar hasta 2 meses antes de la fecha actual, por otro lado si se corre  despuéz del día 15 del mes se extrae hasta 1 mes antes de la fecha actual. Para realizar esta tarea tuvimos que utilizar "Pagination", para poder ir extrayendo subconjuntos de los registros, esto debido a que la API del C5 limita la extracción de los datos a 10000.<br>
* 5) **InsertaMetadatosCLEANED** - esta tarea inserta los metadatos de la tabla "Cleaned", la cual requiere que se limpien los datos  y que se cree la tabla clean de los metadatos.<br>
* 6) **CreaTablaRawJson** - crea la tabla para almacenar los datos en formato JSON al esquema *raw*, que tiene como requerimiento que se cree el esquema *raw*.<br>
* 7) **CreaEsquemaRAW** - crea el esquema *raw* y requiere que se obtenga el host de la RDS para poder conectarse.<br>
* 8) **CreaTablaRawMetadatos** - crea la tabla de los metadatos en el esquema *raw* y a su vez requiere que el esquema *raw* ya este creado.<br>
* 9) **CreaTablaCleanedIncidentes** - Con esto se crea la tabla ya limpia *cleaned* y tiene como requerimiento que el esquema de esa tabla ya este creado.<br>
* 10) **CreaEsquemaCLEANED** - esta tarea crea el esquema de la tabla *cleaned* y tiene como requerimiento que se obtenga el *host* de la RDS.<br>
* 11) **FuncionRemovePoints** - aqui lo que luigi hace es quitar puntos en la base de datos para tener las variables en un buen formato.<br>
* 12) **FuncionUnaccent** - con esta tarea eliminamos acentos a  las variables que lo requieran.<br>
* 13) **CreaTablaCleanedMetadatos** - crea la tabla de los metadatos en el *cleaned*.<br>
* 14) **LimpiaInfoPrimeraVez** - aqui se limpia toda la informacion extraida en la tabla *raw* y se requiere que se cree la tabla de *cleaned*, así como las funciones de limpieza ( puntos y acentos).<br>
* 15) **CreaEsquemaProcesamiento** - se crea el esquema *Procesamiento* para el modelado y tiene como requerimiento que se obtenga el host de la base de datos.<br>
* 16) **CreaEsquemaModelo** - crea el esquema  del *Modelo* y también tiene como requerimiento obtener el el host de la base de datos.<br>
* 17) **CreaTablaModeloMetadatos**- crea la tabla de los metadatos en del esquema *Modelo* y require que ya este el esquema creado.<br>
* 18) **SeleccionaModelo** - En esta tarea se hace el *grid search* de los modelos para elegir el mejor modelo, y tiene como requerimiento la tarea de que la variables tenga el formato *One-hot encoder*. De igual manera sube el archivo *.pkl* a un bucket con los parámetros de la selección del modelo. Hasta el momento se ha realizado únicamente un modelo de *RandomForest<br>
* 19) **DummiesBase** - Aqui luigi convierte las variables categoricas a dummies (One-hot encoder) para la base "Train & Test" y tiene como requerimiento que las variables que lo requiren hayan sido imputadas.<br>
* 20) **ImputacionesBase**- en este punto se hace la imputacion de la base en la "Train & Test" con el requeriiento de que la base haya sido anteriorme separada en entrenamiento y validación.<br>
* 21) **SeparaBase** - Separa la base en la "Train & Test" y requiere que la tarea del preproceso de la base ya haya sido realizada.<br>
* 22) **PreprocesoBase** - Realiza el preprocesamiento de la base y require que el bucket donde se almacenará ya este creado.<br>
* 23) **CreaBucket** - Con esta tarea se crea un bucket en luigi para alancenar el prepocesamiento, y no tiene requerimientos.<br>
* 24) **InsertaMetadatosModelo** - Con esta última tarea se leen el data frame de la metadata generada por el Modelo, y se inserta la tabla de *modelo.Metadatos*, requiere que la el esquema de los metadatos del modelo ya este creado y que se realice la tarea de *SeleccionaModelo*.<br> <hr> 

####  DAG

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/dag_1.png)

### 3. Implicaciones éticas

La ética en nuestro producto de datos tiene dos vertienes principales. En el proceso de ETL, es necesario obtener información para poder analizarla e implementar un modelo para la toma de decisiones. En este caso, la extracción de información proporcionada por el C5 ya se encuentra acotada para porporcionar datos no sensibles, como por ejemplo: los nombres de las personas que realizaron la llamada o el teléfono que realizó la llamada, debido a que es una fuente de datos pública y se reservan ciertos datos, por lo que consideramos que la información que estamos utilizando no presenta un dilema ético para nosotros como científicos de datos, dado que cualquier persona tiene acceso a la misma información.

En segundo lugar, para la implementación del modelo consideramos que si existen implicaciones éticas importantes:

- El modelo desarrollado etiquete incorrectamente un incidente, es decir asigne una probabilidad baja cuando tenga probabilidad alta "verdadero".
- Una asignación ineficiente de los recursos del C5 generaría que el recurso se enviara a otro destino y podría incurrir incluso en alguna pérdida humana.
- Se haga una limpieza incorrecta de las variables para el desarrollo del modelo que afecte los resultados generados.

### 4. Métrica del modelado

En la evaluación del modelo nos enfocaremos en la métrica **"Precision"** para monitorear cuantos incidentes de los que nos arrojo el modelo como *verdaderos* realmente fueron *verdaderos*, esto con la idea de que el producto de datos será proporcionado al C5 con el objetivo de poder asignar mejor sus recursos, por lo que en esta métrica se pueden justamente controlar los *falsos positivos*. Cabe mencionar que la métrica de *Recall*, también puede ser utilizada si lo que se busca es tratar de controlar los *falsos negativos*, es decir mas enfocado en atender a todos los incidentes *verdaderos*. Nuestra métrica tiene fuertes implicaciones éticas como las que se mencionar en el punto anterior, pero justamente depende de cada perspectiva y nosotros nos enfocamos en optimizar los recursos limitados del C5.

De igual manera, es importante mencionar que el producto de datos sólo es una herramienta adicional que será proporcionada al tomador de decisiones del C5 ( por ejemplo un supervisor), el cual se apoyará del producto de datos con la lista de probabilidades priorizadas, pero éste último es el que tomará la decisión si mandar el recurso o no ( o el tipo de recurso) al área solicitada.

## Referencias

C5:
