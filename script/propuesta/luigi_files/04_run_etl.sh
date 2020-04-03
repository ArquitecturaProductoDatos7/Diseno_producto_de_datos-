#!/bin/bash

# Corre hasta la tarea CreaTablasBD
PYTHONPATH="." luigi --module etl_pipeline CreaTablasBD --local-scheduler

# Corre hasta la tarea PeticionApiInfoMensual
#PYTHONPATH="." luigi --module etl_pipeline_met peticion_api_info_mensual --month 1 --year 2019 --local-scheduler