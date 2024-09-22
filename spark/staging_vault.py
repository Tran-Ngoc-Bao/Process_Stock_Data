from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import *
from minio import Minio
from datetime import datetime

run_time = datetime.now().strftime("%Y%m%d%H")
path = "/home/iceberg/code/stagingvault/"
     
if __name__ == "__main__":
    sc = SparkContext("spark://spark-iceberg:7077", "staging_vault")
    spark = SparkSession(sc)

    client = Minio(endpoint="minio:9000", access_key="admin", secret_key="password", secure=False)

    objects = client.list_objects("processing")
    objects_name = []
    for obj in objects:
        objects_name.append(obj.object_name)
        
    for i in objects_name:
        path_parquet = path + i
        client.fget_object("processing", i, path_parquet)
        df = spark.read.parquet(path_parquet)
        if spark.catalog.tableExists("staging_vault." + i.replace("-", "_")):
            df.writeTo("staging_vault." + i.replace("-", "_")).append()
        else:
            df.writeTo("staging_vault." + i.replace("-", "_")).create()
      