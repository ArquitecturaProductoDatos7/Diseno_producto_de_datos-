
## Linaje de los Datos

Con el fin de cumplir con las mejores prácticas en el desarrollo del producto de datos se hara una trazabilidad de como se van modificando los datos desde la etapa raw hasta la ejecución del modelo. Por el monto estamos en la parte del ETL y tenemos tres estapas en las cuales se ira guardando toda la modificación a los datos, con el fin de que se tenga un rastro que cualquier persona pueda descifrar.

## Raw 

La extracción de datos se hace desde la API del C5 y se planea extraer una vez al menos nuevos datos porque la actualización es mensual.

| metadato | Descripción |
| ------------- | ------------- |
| fecha  | Fecha de ejecución  |
| parámetros  | Parámetros utilizados en la ejecución del orquestador  |
| usuario  | Persona que ejecuto el task  |
| origen  | Origen de la ejecución(instancia o IP)  |
| #registros  | Número de registros ingestados con el task  |
| archivo  | Nombre del archivo generado  |
| rutas3  | Ruta de almacenamiento s3 (bucket)  |
| script  | Nombre del script ejecutado  |

## Preprocessed

En esta etapa lo que haremos es pasar los datos en un formato adecuado para poder manipularlos, la API del C5 ya nos proporciona los datos en formato Json y aun no definimos si los pasaremos de Json a Parquet.

| metadato | Descripción |
| ------------- | ------------- |
| fecha  | Fecha de ejecución  |
| usuario  | Persona que ejecuto el procesamiento |
| origen  | Origen de la ejecución(instancia o IP)  |
| acción  | Indicamos que pasamos los datos de formato Json a Parquet  |
| #registros  | Número de registros modificados  |
| estatus  | Indicador de la transformación ( exitoso o fallido)  |

## Clean

En esta fase haremos limpieza de datos para poder implementar el EDA para identificar las variables que se quedaran para el desarrollo del modelo.

| metadato | Descripción |
| ------------- | ------------- |
| fecha  | Fecha de ejecución  |
| usuario  | Persona que ejecuto el task de limpieza |
| origen  | Origen de la ejecución(instancia o IP)  |
| acción  | Indicamos que tarea se realizo (eliminación duplicados, transformación de texto, pivoteo)  |
| #registros  | Número de registros limpios  |
| estatus  | Indicador de limpieza ( exitoso o fallido)  |




