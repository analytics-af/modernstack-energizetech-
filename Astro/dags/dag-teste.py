from airflow.decorators import dag, task
from datetime import datetime, timedelta

@task()
def dizer_ola():
    print("Olá, Mundo!")

@dag(
    dag_id='dag_ola_mundo',
    description='Uma DAG simples de Olá Mundo',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def dag_ola_mundo():
    dizer_ola()

dag_ola_mundo()


