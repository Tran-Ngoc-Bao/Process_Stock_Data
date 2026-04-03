# Project Phase 2 - Data Engineering - Viettel Digital Talent 2024

## Project introduction
<ul>
  <li>Name of project: Process stock data</li>
  <li>Project objective:
    <ul>
      <li>Building a near real-time stock data processing system</li>
      <li>Building a data lakehouse for big data</li>
    </ul>
  </li>
</ul>

## Data flow
  <img src="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/pictures/system.png">

## Deploy system
#### 1. Start system
```sh
docker compose up -d
```

#### 2. Start DAG on Airflow webserver
  
#### 3. Visualize data in Superset with SQLalchemy uri
```
trino://hive@trino:8080/iceberg
```

## ETL
  <img src="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/pictures/etl.png">

## Report
<ul>
  <li><a href="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/pictures/slide.pptx">Slide</a></li>
  <li><a href="https://github.com/Tran-Ngoc-Bao/Process_Stock_Data/blob/master/pictures/demo.mp4">Video demo</a></li>
</ul>
