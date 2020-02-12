
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

## Referencias

C5:
