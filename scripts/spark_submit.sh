#!/bin/ksh

ctl_file=../ctl/ctl_file1.json

spark-submit --jars ../lib/*.jar ./spark_ingest_app.py --ctl $ctl_file


