import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from app.config.config_loader import get_spark_conf
from app.etl.run import Run

sparkManager = SparkSession.builder.config(conf=get_spark_conf('LOCAL'))

spark = configure_spark_with_delta_pip(sparkManager).enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

date = sys.argv[1] if len(sys.argv) > 1 else '2021-03-21'

run = Run(spark, date)

run.initialize()

spark.stop()
