# Project Phase 2 - Data Engineering - Viettel Digital Talent 2024

## Project introduction
<ul>
  <li>Name of project: Process stock data</li>
  <li>Project objective:
    <ul>
      <li>Building a near real-time stock data processing system</li>
      <li></li>
      <li></li>
    </ul>
  </li>
</ul>

## Data flow
  <img src="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/pictures/system.png">

## Deploy system
#### 1. You should pull and build images in file docker-compose.yaml before

#### 2. Move to clone project and Start system
  
```sh
docker compose up -d
```

#### 3. Build enviroment on airflow-webserve and airflow-scheduler

```sh
docker exec -u root -it [airflow-webserver/airflow-scheduler] bash 
source /opt/airflow/code/build-env.sh
```

#### 4. Install minio on spark-iceberg

```sh
docker exec -u root -it spark-iceberg pip install minio
```

#### 5. After start system, all port website of containers in <a href="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/port.txt">here</a>

#### 6. Start DAG in Airflow cluster

#### 7. Build enviroment Superset
```sh
./superset/bootstrap-superset.sh
```
  
#### 8. Visualize data in Superset with SQLalchemy uri
```sh
trino://hive@trino:8080/iceberg
```

## Demo


## Report
<ul>
  <li><a href="">Slide</a></li>
</ul>
