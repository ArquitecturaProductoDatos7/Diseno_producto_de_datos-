#!/bin/bash

# Corre hasta la tarea CreaEsquemaCLEAN
 PYTHONPATH='.' luigi --module etl_pipeline_ver4 CreaEsquemaCLEAN --local-scheduler
