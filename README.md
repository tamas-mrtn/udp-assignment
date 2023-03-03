# UDP Hiring Assignment

## Setup

Install requirements
```
pip install -r requirements.txt
```

Generate dags:
```
python ./include/generate_dag_files.py
```

Starting airflow:
```
docker compose up airflow-init
docker compose up
```

Safely killing airflow:
```
docker compose down --volumes --remove-orphans
```
