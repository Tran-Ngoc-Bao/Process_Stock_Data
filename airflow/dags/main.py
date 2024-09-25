from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import os
import json
from minio import Minio
import pandas

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 25),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes = 1),
}

# dag = DAG("main", default_args = default_args, schedule_interval = "0 3,4,7,8 * * *")
dag = DAG("main", default_args = default_args, schedule_interval = timedelta(30))


client = Minio(endpoint="minio:9000", access_key="admin", secret_key="password", secure=False)
run_time = datetime.now().strftime("%Y%m%d%H")

def e_l_t_i():
    if not client.bucket_exists("inprogress"):
        client.make_bucket("inprogress")
    
    os.system("curl https://iboard-api.ssi.com.vn/statistics/charts/defaultAllStocksV2 > /opt/airflow/code/inprogress/allstocks.json")

    file = open("/opt/airflow/code/inprogress/allstocks.json", "r", encoding="utf-8")
    data = json.load(file)

    for i in data["data"]:
        if i["type"] == "i":
            code = i["code"]
            path_group = "/opt/airflow/code/inprogress/group/{}.json".format(code)
            path_exchange_index = "/opt/airflow/code/inprogress/exchange-index/{}.json".format(code)

            os.system("curl https://iboard-query.ssi.com.vn/v2/stock/group/{} > ".format(code) + path_group)
            os.system("curl https://iboard-query.ssi.com.vn/exchange-index/{}?hasHistory=true > ".format(code) + path_exchange_index)

            client.fput_object("inprogress", "group/" + code + ".json", path_group)
            client.fput_object("inprogress", "exchange-index/" + code + ".json", path_exchange_index)

extract_load_to_inprogress = PythonOperator(
    task_id = "extract_load_to_inprogress",
    python_callable = e_l_t_i, 
    dag = dag
)

def sub_cjtptp(prefix):
    objects = client.list_objects("inprogress", prefix=prefix, recursive=True)
    objects_name = []
    for obj in objects:
        objects_name.append(obj.object_name)

    path = "/opt/airflow/code/processing/"
    if prefix == "exchange-index":
        path_json = path + objects_name[0]
        client.fget_object("inprogress", objects_name[0], path_json)
        file = open(path_json, "r", encoding="utf-8")
        data = json.load(file)
        big_df = pandas.DataFrame(data["data"]["history"])
        big_df["indexId"] = data["data"]["indexId"].upper()
        
        data["data"]["indexId"] = data["data"]["indexId"].upper()
        data["data"]["history"] = 0
        summary = []
        summary.append(data["data"])

        for i in range(1, len(objects_name)):
            path_json = path + objects_name[i]
            client.fget_object("inprogress", objects_name[i], path_json)
            file = open(path_json, "r", encoding="utf-8")
            data = json.load(file)
            df = pandas.DataFrame(data["data"]["history"])
            df["indexId"] = data["data"]["indexId"].upper()
            big_df = pandas.concat([big_df, df], ignore_index=True)

            data["data"]["indexId"] = data["data"]["indexId"].upper()
            data["data"]["history"] = 0
            summary.append(data["data"])
        
        path_parquet = path + prefix + "_total"
        big_df["ETL_time"] = run_time
        big_df["exchange"] = data["data"]["exchange"]
        big_df.to_parquet(path_parquet)
        client.fput_object("processing", prefix, path_parquet)

        path_parquet = path + "summary"
        df = pandas.DataFrame(summary)
        df["ETL_time"] = run_time
        df.to_parquet(path_parquet)
        client.fput_object("processing", "summary", path_parquet)
    elif prefix == "group":
        path_json = path + objects_name[0]
        client.fget_object("inprogress", objects_name[0], path_json)
        file = open(path_json, "r", encoding="utf-8")
        data = json.load(file)
        big_df = pandas.DataFrame(data["data"])
        big_df["indexId"] = objects_name[0][6:(len(objects_name[0]) - 5)]

        for i in range(1, len(objects_name)):
            path_json = path + objects_name[i]
            client.fget_object("inprogress", objects_name[i], path_json)
            file = open(path_json, "r", encoding="utf-8")
            data = json.load(file)
            df = pandas.DataFrame(data["data"])
            df["indexId"] = objects_name[i][6:(len(objects_name[i]) - 5)]
            big_df = pandas.concat([big_df, df], ignore_index=True)

        path_parquet = path + prefix + "_total"
        big_df["ETL_time"] = run_time
        big_df.to_parquet(path_parquet)
        client.fput_object("processing", prefix, path_parquet)

def sub_trino():
    file = open("/opt/airflow/code/source/call.sql", "w")
    data = """
    create schema if not exists iceberg.raw_vault;
    use iceberg.raw_vault;
    call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'summary_{run_time}', table_location => 's3a://warehouse/staging_vault/summary_{run_time}');
    call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'gr_{run_time}', table_location => 's3a://warehouse/staging_vault/group_{run_time}');
    call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'exchange_index_{run_time}', table_location => 's3a://warehouse/staging_vault/exchange_index_{run_time}');
    insert into summary
    select *
    from summary_{run_time};
    insert into gr
    select *
    from gr_{run_time};
    insert into exchange_index
    select *
    from exchange_index_{run_time};
    drop table summary_{run_time};
    drop table exchange_index_{run_time};
    drop table gr_{run_time};
    """.format(run_time=run_time)
    file.write(data)
    file.close()

    # create table if not exists summary
    # as select *
    # from summary_{run_time};
    # create table if not exists gr
    # as select *
    # from gr_{run_time};
    # create table if not exists exchange_index
    # as select *
    # from exchange_index_{run_time};

def c_j_t_p_t_p():
    if not client.bucket_exists("processing"):
        client.make_bucket("processing")

    sub_cjtptp("group")
    sub_cjtptp("exchange-index")

    sub_trino()

convert_json_to_parquet_to_processing = PythonOperator(
    task_id = "convert_json_to_parquet_to_processing",
    python_callable = c_j_t_p_t_p, 
    dag = dag
)

spark_convert_parquet_to_iceberg_to_minio = BashOperator(
    task_id = "spark_convert_parquet_to_iceberg_to_minio",
    bash_command = 'sleep 60',
    # bash_command = 'spark-submit /opt/airflow/code/source/staging_vault.py', 
    dag = dag
)

def m_f_t_a():
    if not client.bucket_exists("archive"):
        client.make_bucket("archive")

    objects = client.list_objects("processing")
    objects_name = []
    for obj in objects:
        objects_name.append(obj.object_name)

    path = "/opt/airflow/code/archive/"
    for i in objects_name:
        path_parquet = path + i
        client.fget_object("processing", i, path_parquet)
        client.fput_object("archive", i + "/" + run_time, path_parquet)

move_file_to_archive = PythonOperator(
    task_id = "move_file_to_archive",
    python_callable = m_f_t_a, 
    dag = dag
)

def sub_dfip(prefix):
    objects = client.list_objects("processing")
    objects_name = []
    for obj in objects:
        objects_name.append(obj.object_name)

    for i in objects_name:
        client.remove_object("processing", i)

    objects = client.list_objects("inprogress", prefix=prefix, recursive=True)
    objects_name = []
    for obj in objects:
        objects_name.append(obj.object_name)

    for i in objects_name:
        client.remove_object("inprogress", i)

def d_f_i_p():
    sub_dfip("group")
    sub_dfip("exchange-index")

delete_file_inprogress_processing = PythonOperator(
    task_id = "delete_file_inprogress_processing",
    python_callable = d_f_i_p,
    dag = dag
)

trino_create_rawvault = BashOperator(
    task_id = "trino_create_rawvault",
    # bash_command = 'ls -l',
    bash_command = 'cd /opt/airflow/code && ./trino --server http://trino:8080 --file source/call.sql && ./trino --server http://trino:8080 --file source/raw_vault.sql', 
    dag = dag
)

trino_create_businessvault = BashOperator(
    task_id = "trino_create_businessvault",
    # bash_command = 'ls -l',
    bash_command = 'cd /opt/airflow/code && ./trino --server http://trino:8080 --file source/business_vault.sql', 
    dag = dag
)

trino_create_starschemakimball = BashOperator(
    task_id = "trino_create_starschemakimball",
    # bash_command = 'ls -l',
    bash_command = 'cd /opt/airflow/code && ./trino --server http://trino:8080 --file source/star_schema_kimball.sql', 
    dag = dag
)

trino_create_datamart = BashOperator(
    task_id = "trino_create_datamart",
    # bash_command = 'ls -l',
    bash_command = 'cd /opt/airflow/code && ./trino --server http://trino:8080 --file source/data_mart.sql', 
    dag = dag
)

extract_load_to_inprogress >> convert_json_to_parquet_to_processing >> spark_convert_parquet_to_iceberg_to_minio >> move_file_to_archive >> delete_file_inprogress_processing
spark_convert_parquet_to_iceberg_to_minio >> trino_create_rawvault >> trino_create_businessvault >> trino_create_starschemakimball >> trino_create_datamart