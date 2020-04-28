#!/bin/bash

# Corre las tareas del ETLpipeline
 PYTHONPATH='.' luigi --module etl_pipeline_ver6 ETLpipeline --local-scheduler
