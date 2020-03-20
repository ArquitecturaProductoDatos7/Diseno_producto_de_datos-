
## Linaje de los Datos

Con el fin de cumplir con las mejores prácticas en el desarrollo del producto de datos se hara una trazabilidad de como se van modificando los datos desde la etapa raw hasta la ejecución del modelo. Por el monto estamos en la parte del ETL y tenemos tres estapas en las cuales se ira guardando toda la modificación a los datos, con el fin de que se tenga un rastro que cualquier persona pueda descifrar.

## Etapa Raw

La extracción de datos se hace desde la API del c5 y se planea extraer una vez al menos nuevos datos porque la actualización es mensual.

| metadato | Descripción |
| ------------- | ------------- |
| fecha  | Fecha de ejecución  |
| parámetros  | Parámetros utilizados en la ejecución del orquestador  |
| usuario  | Persona que ejecuto el task  |
| destino  | Origen de la ejecución(instancia o IP)  |
| #registros  | Número de registros ingestados en el task  |
| archivo  | Nombre del archivo generado  |
| rutas3  | Ruta de almacenamiento s3 (bucket)  |
| script  | Nombre del script ejecutado  |
