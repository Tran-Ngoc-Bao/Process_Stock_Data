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

#### 3. Build enviroment on airflow-webserve

```sh
docker exec -it -u root airflow-webserver source /opt/airflow/code/build-env.sh
```

#### 4. Build enviroment on airflow-scheduler

```sh
docker exec -it -u root airflow-scheduler source /opt/airflow/code/build-env.sh
```

#### 5. After start system, all port website of containers in <a href="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/port.txt">here</a>
#### 6. Start DAG in Airflow cluster
#### 7. Move to folder superset and run

```sh
./bootstrap-superset.sh
```
  
#### 7. Visualize data in Superset website on local

## Demo


## Report
<ul>
  <li><a href="">Report</a></li>
  <li><a href="">Slide</a></li>
</ul>
