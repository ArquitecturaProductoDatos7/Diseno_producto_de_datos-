
## EDA (Exploratory Data Analysis)

##### Fuente de la base de datos
Centro de Comando, Control, Cómputo, Comunicaciones y Contacto Ciudadano de la Ciudad de México (C5)

### Objetivo del problema
Realizar un modelo para precedir si la llamada que recibe el Centro de Comdando (C5) es **verdadera**, esto con el fin de lograr una mejor asiganción de recursos de este organismo, debido a que muchas de las llamadas que reciben son falsas.

### Diccionario de datos

folio: El folio único que tiene cada llamada que se registra
fecha_creacion: Fecha en la que se realizó la llamada
hora_creacion: Hora de creación de la llamada telefónica
dia_semana: El día de la semana que se realizó la llamada(Lunes a Domingo)
fecha_cierre: Fecha del cierre del reporte
año_cierre:Año en el que se cerró el reporte de la llamada
mes_cierre:Mes en el que se cerró el reporte de la llamada
hora_cierre: Hora en la que se cerró el reporte de la llamada
delegacion_inicio: Delegación en la que sucedió el accidente
incidente_c4:Tipo de incidente reportado
latitud: De donde sucedió el incidente
longitud: De donde sucedió el incidente
codigo_cierre: Clasificación de la llamada de acuerdo a un código de cierre

A = “Afirmativo”: Una unidad de atención a emergencias fue despachada, llegó al lugar de los hechos y confirmó la emergencia reportada.
N = “Negativo”: Una unidad de atención a emergencias fue despachada, llegó al lugar de los hechos, pero en el sitio del evento nadie confirmo la emergencia ni fue solicitado el apoyo de la unidad.
I = “Informativo”: Corresponde a solicitudes de información.
F = “Falso”: El incidente reportado inicialmente fue considerado como falso en el lugar de los hechos.
D = “Duplicados”: El incidente reportado se registró en dos o más ocasiones procediendo a mantener un solo reporte como el original.

clas_con_f_alarma:Clasificacion de la alarma reportada
tipo_entrada:Entrada con la cual se registro la llamada
delegacion_cierre:Delegación en la que se cerró el reporte de la llamada
geopoint:Geolocalización del incidente reportado
mes:Mes en número en el que se realizó la llamada.

#### 1) Conocimiento de variables y limpieza de los datos
Se encontró que la base proporcionada por la API del C5 contiene 18 variables y 1,303,778 registros hasta su última actualización( se actualiza cada mes y los datos proporcionados son desde el año 2014.

##### Comentarios
a)Las variables son principalmente categóricas o descriptivas, la única variables numéricas con las que buena el **raw** son Año_cierre y mes. Debido a estó no fue posible hacer gráficas directas de correlación para descartar ciertas variables para el modelo, se tuvieron que hacer ciertas transformaciones.
b)Se creo la variable hora_entera porque consideramos que con tener la hora entera sin minutos y segundos es más factible de manipular.
c)Se cambiaron los formatos a "datetime64[ns]" de las variables fecha_creación, hora_creación y fecha_cierre y hora_cierre; dado que tenían el tipo de variable "object".

#### 2) Data Profiling

##### Variables numéricas
En cuanto a las variables numéricas se encontró que no había registros faltantes y el resumen de estadísticas descriptivas no arrojó información muy valiosa porque las varibles son de tiempo : año_cierre y mes.

##### Variables categóricas
Se encontró que tenemos registros faltantes en las siguientes variables : hora_cierre (647), delegación_inicio (156) y delegación_cierre (138) porque lo se debe considerar en el modelo este hallazgo. 

De igual manera se creó la variable target (dummy) que indica un 1 si la llamada registrada fue **afirmativa** y 0 en los demás casos. Consideramos únicamente como 1 a las llamadas registradas con el código de "a", debido a que las que son clasificadas con "i" realmente son informativas y proporcionan información adicional a una llamada ya registrada.

#### 3) Análisis Gráfico Exploratorio








