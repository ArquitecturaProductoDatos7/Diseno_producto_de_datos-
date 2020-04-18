#!/bin/bash

# Corre las tareas del ETLpipeline
 PYTHONPATH='.' luigi --module etl_pipeline_ver5 ETLpipeline --local-scheduler
