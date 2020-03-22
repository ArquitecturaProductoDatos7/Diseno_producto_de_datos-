
## Linaje de los Datos

Con el fin de cumplir con las mejores prácticas en el desarrollo del producto de datos se hara una trazabilidad de como se van modificando los datos desde la etapa *raw* hasta la ejecución del modelo. En la parte del ETL se tienen tres estapas en las cuales se guardará información relacionada a la modificación de los datos, con el fin de que se tenga un rasteabilidad que cualquier persona pueda descifrar.

## 1. Raw 

La extracción de datos se hace desde la API de datos abiertos de la CDMX y se planea extraer una vez al menos nuevos datos porque la actualización es mensual. Aquí se tendrán los datos en crudo, es decir, sin modificación alguna. En esta etapa, se guardarán la siguiente información: 

| Metadato | Descripción |
| ------------- | ------------- |
| fecha  | Fecha de ejecución  |
| parámetros  | Parámetros utilizados en la ejecución del orquestador  |
| usuario  | Persona que ejecuto el task  |
| origen  | Origen de la ejecución(instancia o IP)  |
| num_registros  | Número de registros ingestados con el task  |
| archivo  | Nombre del archivo generado  |
| rutas3  | Ruta de almacenamiento s3 (bucket)  |
| script  | Nombre del script ejecutado  |

## 2. Preprocessed

En esta etapa lo que haremos es pasar los datos en un formato adecuado para poder manipularlos, la API ya nos proporciona los datos en formato JSON y aun no definimos si los pasaremos de JSON a Parquet.

| metadato | Descripción |
| ------------- | ------------- |
| fecha  | Fecha de ejecución  |
| usuario  | Persona que ejecuto el procesamiento |
| origen  | Origen de la ejecución(instancia o IP)  |
| acción  | Indicamos que pasamos los datos de formato Json a Parquet  |
| num_registros  | Número de registros modificados  |
| estatus  | Indicador de la transformación ( exitoso o fallido)  |

## 3. Clean

En esta fase haremos limpieza de datos para poder implementar el EDA para identificar las variables que se quedaran para el desarrollo del modelo.

| metadato | Descripción |
| ------------- | ------------- |
| fecha  | Fecha de ejecución  |
| usuario  | Persona que ejecuto el task de limpieza |
| origen  | Origen de la ejecución(instancia o IP)  |
| acción  | Indicamos que tarea se realizo (eliminación duplicados, transformación de texto, pivoteo)  |
| num_registros  | Número de registros limpios  |
| estatus  | Indicador de limpieza ( exitoso o fallido)  |




