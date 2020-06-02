
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
     - Eliminar acentos.<br>
* Con las transformaciones anteriores se pasará al esquema *cleaned* en la RDS. En esta parte, de acuerdo al EDA (Exploratory Data Analysis) las variables que se quedarón para el modelo son las siguientes: hora_creacion (únicamente la hora, sin minutos ni segundos), delegacion_inicio, dia_semana, tipo_entrada, mes, incidente_c4 y la variable target (codigo_cierre). 
Se puede encontrar el diccionario de estas variables en la siguiente liga [EDA](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/tree/master/EDA).<br> <hr> 

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/EtlAjustada.png)

### 2. Orchestration

Ocuparemos Luigi como orquestador de las tareas que vamos a ir realizando en el producto de datos, hasta el momento tenemos las siguientes tareas:<br>
* 1) **EtlPipeline** - que se encarga de inicializar las tareas de *ObtieneRDSHost*, *ExtraeInfoPrimeraVez* hasta  *InsertaMetadatosCLEANED*.<br>
* 2) **CreaInstanciaRDS** - con esta tarea se crea la base de datos en postgres cuando se obtiene un "Subnet Group".<br>
* 3) **ObtieneRDSHost** - con la cual se obtiene el endpoint(host) de la RDS creada para poder hacer la conexión posteriormente y requiere que la base de datos en postgres ya este creada.<br>
* 4) **CreaEsquemaRAW** - crea el esquema *raw* y requiere que se obtenga el host de la RDS para poder conectarse.<br>
* 5) **CreaTablaRawJson** - crea la tabla para almacenar los datos en formato JSON, que tiene como requerimiento que se cree el esquema *raw*.<br>
* 6) **CreaTablaRawMetadatos** - crea la tabla de los metadatos en el esquema *raw* y a su vez requiere que el esquema *raw* ya este creado.<br>
* 7) **ExtraeInfoPrimeraVez** - Aqui luigi lo que hace es extraer toda la informacion desde la API del C5 (desde el 1-Ene-2014), el rango superior de la extracción depende de la fecha en que se corre el pipeline. Si se corre antes del 15 de mes se extrae hasta 2 meses antes de la fecha actual, por otro lado si se corre  después del día 15 del mes se extrae hasta 1 mes antes de la fecha actual. Para realizar esta tarea tuvimos que utilizar "Pagination", para poder ir extrayendo subconjuntos de los registros, esto debido a que la API del C5 limita la extracción de los datos a 10000.<br>
* 8) **InsertaMetadatosRAW** - esta tarea inserta los metadatos de raw, la cual requiere que se extraigan los datos  y que se cree la tabla de los metadatos.<br>
* 9) **CreaEsquemaCLEANED** - esta tarea crea el esquema cleaned y tiene como requerimiento que se obtenga el *host* de la RDS.<br>
* 10) **CreaTablaCleanedIncidentes** - Con esto se crea la tabla para insertar los datos limpios y tiene como requerimiento que el esquema de esa tabla ya este creado.<br>
* 11) **CreaTablaCleanedMetadatos** - crea la tabla de los metadatos en el esquema *cleaned*.<br>
* 12) **FuncionRemovePoints** - aqui lo que luigi hace es quitar puntos, diagonales y otros caracteres especiales en la base de datos para tener las variables en un buen formato.<br>
* 13) **FuncionUnaccent** - con esta tarea eliminamos acentos a las variables que lo requieran.<br>
* 14) **LimpiaInfoPrimeraVez** - aqui se limpia toda la informacion extraida y se requiere que se cree la tabla de *cleaned*, así como las funciones de limpieza (puntos y acentos).<br>
* 15) **InsertaMetadatosCLEANED** - esta tarea inserta los metadatos de Cleaned, la cual requiere que se limpien los datos  y que se cree la tabla cleaned de los metadatos.<br>
* 16) **CreaEsquemaProcesamiento** - se crea el esquema *Procesamiento* para iniciar el tratamiento a las variables que se meten al modelo y tiene como requerimiento que se obtenga el host de la base de datos.<br>
* 17) **CreaEsquemaModelo** - crea el esquema  del *Modelo* y también tiene como requerimiento obtener el el host de la base de datos.<br>
* 18) **CreaTablaFeatuEnginMetadatos** - crea la tabla de los metadatos en el esquema *procesamiento*.<br>
* 19) **CreaTablaModeloMetadatos**- crea la tabla de los metadatos en del esquema *Modelo* y require que ya este el esquema creado.<br>
* 20) **CreaBucket** - Con esta tarea se crea un bucket en AWS para alancenar el prepocesamiento de las variables y los modelos probados, y no tiene requerimientos.<br>
* 21) **PreprocesoBase** - Realiza el preprocesamiento de la base (creación de otras variables, recategorización, etc.) y require que el bucket donde se almacenará ya este creado.<br>
* 22) **SeparaBase** - Separa la base en "Train & Test" y requiere que la tarea del preproceso de la base ya haya sido realizada.<br>
* 23) **ImputacionesBase**- en este punto se hace la imputacion de la base en "Train & Test" con el requerimiento de que la base haya sido anteriorme separada en entrenamiento y prueba.<br>
* 24) **DummiesBase** - Aqui luigi convierte las variables categoricas a One-hot encoder para la base "Train & Test" y tiene como requerimiento que las variables que lo requiren hayan sido imputadas.<br>
* 25) **InsertaMetadatosFeatuEngin** - esta tarea inserta los metadatos de feature engineering, la cual requiere que se preprocese, separe, impute y se creen las one-hote encoder variables de la base.<br>
* 26) **SeleccionaModelo** - En esta tarea se hace el *grid search* de los modelos para elegir el mejor modelo, y tiene como requerimiento la tarea de que la variables tenga el formato *One-hot encoder*. De igual manera sube el archivo *.pkl* a un bucket con los parámetros de la selección del modelo. Hasta el momento se ha realizado únicamente un modelo de *RandomForest<br>
* 27) **InsertaMetadatosModelo** - Con esta última tarea se leen el data frame de la metadata generada por el Modelo, y se inserta la tabla de *modelo.Metadatos*, requiere que la el esquema de los metadatos del modelo ya este creado y que se realice la tarea de *SeleccionaModelo*.<br> <hr> 

####  DAG

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/dag_1.png)

### 3. Implicaciones éticas

La ética en nuestro producto de datos tiene dos vertienes principales. En el proceso de ETL, es necesario obtener información para poder analizarla e implementar un modelo para la toma de decisiones. En este caso, la extracción de información proporcionada por el C5 ya se encuentra acotada para porporcionar datos no sensibles, como por ejemplo: los nombres de las personas que realizaron la llamada o el teléfono que realizó la llamada, debido a que es una fuente de datos pública y se reservan ciertos datos, por lo que consideramos que la información que estamos utilizando no presenta un dilema ético para nosotros como científicos de datos, dado que cualquier persona tiene acceso a la misma información.

En segundo lugar, para la implementación del modelo consideramos que sí existen implicaciones éticas importantes:

- El modelo desarrollado etiquete incorrectamente un incidente, es decir asigne una probabilidad baja de ser verdadero a un incidente que sea real.
- Una asignación ineficiente de los recursos del C5 generaría que el recurso se enviara a otro destino y podría incurrir incluso en alguna pérdida humana.
- Que para el modelo, no se tome en cuenta la información de ciertas variables que pudieran ser importantes.
- Se haga una limpieza o reclasificación incorrecta de las variables para el desarrollo del modelo que afecte los resultados generados.

### 4. Modelo

Después de evaluar los 3 modelos (Random Forest, Regresión Logística y Xgboost), junto con el grid search de cada uno se obtuvo que las mejores predicciones eran con el modelo XGboost, por lo que fue el modelo con el cual se realizaron las probabilidades para definir la nueva etiqueta de la variable target. 

### 5. Métrica del modelado

Para medir el desempeño del modelo nos enfocaremos en la métrica **"Precision"** para monitorear cuantos incidentes de los que nos arrojo el modelo como *verdaderos* realmente fueron *verdaderos*, esto con la idea de que el producto de datos será proporcionado al C5 con el objetivo de poder asignar mejor sus recursos limitados, por lo que en esta métrica se pueden justamente controlar los *falsos positivos*. Cabe mencionar que la métrica de *Recall*, también puede ser utilizada si lo que se busca es tratar de controlar los *falsos negativos*, es decir más enfocado en atender a todos los incidentes *verdaderos*. Nuestra métrica tiene fuertes implicaciones éticas como la que se menciona en el punto anterior, pero justamente depende de cada perspectiva y nosotros nos enfocamos en optimizar los recursos limitados del C5.

De igual manera, es importante mencionar que el producto de datos sólo es una herramienta adicional que será proporcionada al tomador de decisiones del C5 (por ejemplo un supervisor), el cual se apoyará del producto de datos con la lista de probabilidades priorizadas, pero éste último es el que tomará la decisión si mandar el recurso o no (o el tipo de recurso) al área solicitada.

### 6. *Bias y Fairness*

##### 6.1 Atributo Protegido

El producto de datos desarrollado para el C5, tiene como objetivo etiquetar las llamadas entrantes al centrol de control con una probabilidad asignada determinada con un modelo de predicción. De acuerdo a la base de datos se decidió tomar como atributo protegido la variable *delegación_inicio*, y se elige esta variable porque queremos evitar un sesgo socioeconómico, en el sentido de que las delegaciones de la Cuidad de México son heterogéneas ya que pueden ser distintas por su nivel de infraestructura, características de su población o su ubicación. Y justamente lo que se desea evitar es que existan delegaciones marginadas o no atendidas por el simple hecho de tener atributos que la beneficien en comparación con otras.

##### 6.2 Métricas *Bias* y *Fairness*

Se determinó que el producto de datos es tipo **assitive** porque es una intervención cuyo objetivo es proporcionar una lista de probabilidades priorizada para determinar que llamadas entrantes al C5 son *verdaderas*. Se eligieron 2 métricas para evaluar el sesgo en el modelo: 
* **False Negative Rate** - Se busca que todas las delegaciones  (atributo protegido) tengan el mismo FNR, es decir no privilegar a ciertas delegaciones sobre otras en cuanto a atención.
![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/FNR.png)

Se acuerdo con esta primer métrica se observa que por ejemplo en las delegaciones de Tlalpan y Álvaro Obregón son en las que el modelo predice incorrectamente en mayor número de veces. En el caso de Tlalpan el 92% de la veces las llamadas que eran *verdaderas* la etiqueta con baja probabilidad o no *verdaderas*, justo por el hecho de priorizar las que tengan mayor probabilidad.

* **False Omission Rate** - Para medir la proporción de falsos negativos que se rechazan incorrectamente y en este caso interesa conocer su hay un sesgo hacia alguna delegación en este sentido, debido a que se busca tener unna paridad de FNR (primera mètrica) en todas las delegaciones.

![alt text](https://github.com/ArquitecturaProductoDatos7/Diseno_producto_de_datos-/blob/master/imagenes/FOR.png)

De igual manera en la gráfica del FOR se tienen 2 delegaciones con esa métrica alta, estas son Milpa y Cuahutemoc en las cuales hay mayor numero de casos en los que se los falsos negativos son etiquetados incorrectamente como falsos.

De igual manera, se obutuvo el **FOR disparity** y el **FNR disparity** para completar el análsis y poder realizar una comparación entre una delegación como punto de referencia para determinar la disparidad entre todas las delegaciones respecto al *benchmark* elegido. En este caso la delegación de referencia fue Iztapalapa que es la delegación que tiene mayor número de ocurrencias y justamente puede reflejar un mayor acercamiento del comportamiento de los datos para el análisis. 

## Referencias

[C5](https://datos.cdmx.gob.mx/explore/dataset/incidentes-viales-c5/table/?disjunctive.incidente_c4) <br>
[Aequitas](https://dssg.github.io/aequitas/examples/compas_demo.html)
