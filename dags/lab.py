from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'run_script_dag',
    default_args=default_args,
    description='First DAG',
    schedule_interval='0 0 */5 * *',  
    start_date=datetime(2024, 7, 25, 8, 38),
    catchup=False
)

run_script = BashOperator(
    task_id='run_script_task',
    bash_command='docker exec spark-sql-and-pyspark-using-python3-itvdelab-1 python3 /home/itversity/itversity-material/python/move_hdfs.py',
    dag=dag,

)

run_script
