#!/bin/bash

export MSYS_NO_PATHCONV=1

docker exec spark-runner /opt/spark/bin/spark-submit \
  --driver-memory 4g \
  --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
  --conf "spark.executor.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2" \
  --packages io.openlineage:openlineage-spark_2.12:1.47.1 \
  --conf "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener" \
  --conf "spark.openlineage.transport.url=http://host.docker.internal:5000" \
  --conf "spark.openlineage.transport.type=http" \
  --conf "spark.openlineage.namespace=tlc_etl"\
  /app/app.py