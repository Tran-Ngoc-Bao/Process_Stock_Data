from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import *
from minio import Minio
from datetime import datetime

run_time = datetime.now().strftime("%H%d%m%Y")
path = "/home/iceberg/code/stagingvault/"
     
def sub_main(prefix):
    objects = client.list_objects("archive", prefix=prefix, recursive=True)
    objects_name = []
    for obj in objects:
        objects_name.append(obj.object_name)
        
    for i in objects_name:
        path_parquet = path + i
        client.fget_object("archive", i, path_parquet)
        df = spark.read.parquet(path_parquet)
        df.writeTo(prefix.replace("-", "_") + "." + i[(len(prefix) + 1):] + "_" + run_time).createOrReplace()

if __name__ == "__main__":
	sc = SparkContext("spark://spark-iceberg:7077", "staging_vault")
	spark = SparkSession(sc)

	client = Minio(endpoint="minio:9000", access_key="admin", secret_key="password", secure=False)
	sub_main("group")
	sub_main("odd-exchange")
	sub_main("put-exec")
	sub_main("exchange-index")
	sub_main("summary")
      