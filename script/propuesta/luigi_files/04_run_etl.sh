#!/bin/

python -m luigi --module etl_pipeline_met peticion_api_info_mensual --month 1 --year 2019 --local-scheduler