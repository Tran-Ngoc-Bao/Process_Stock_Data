# Project Phase 2 - Data Engineering - Viettel Digital Talent 2024

## Project introduction
<ul>
  <li>Name of project: Process stock data</li>
  <li>Project objective:
    <ul>
      <li></li>
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

#### 3. Install java on airflow-webserve

```sh
docker exec -it -u root airflow-webserver /bin/bash
apt update && apt install default-jdk
```

#### 4. After start system, all port website of containers in <a href="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/port.txt">here</a>
#### 5. Start DAG in Airflow cluster
#### 6. Move to folder superset and run

```sh
bash bootstrap-superset.sh
```
  
#### 7. Visualize data in Superset website on local

## Demo


## Report
<ul>
  <li><a href="">Report</a></li>
  <li><a href="">Slide</a></li>
</ul>
