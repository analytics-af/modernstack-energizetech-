# Função para extrair dados do MongoDB e carregar no PostgreSQL
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from pymongo import MongoClient
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from dotenv import load_dotenv
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime
import os

#Credenciais .env
load_dotenv()

# Argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=2),
}

def truncar_todas_tabelas_schema_public():
    # Conexão com PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
    cur = conn.cursor()

    try:
        # Obter todas as tabelas do schema public
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tabelas = cur.fetchall()

        # Truncar todas as tabelas
        for tabela in tabelas:
            nome_tabela = tabela[0]
            cur.execute(f"TRUNCATE TABLE {nome_tabela}")
            print(f"A tabela {nome_tabela} foi truncada.")

        conn.commit()
        print("Todas as tabelas do schema public foram truncadas com sucesso.")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao truncar tabelas: {str(e)}")
    finally:
        cur.close()
        conn.close()

def limpar_dados_fatos():
    # Conexão com PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
    cur = conn.cursor()

    try:
        # Tabelas fato , Data da Fato
        tabelas_fato = [
            ("dw_comercial.fato_vendas", "data_venda"),
            ("dw_comercial.fato_devolucoes", "data_devolucao")
        ]

        # Obter a data atual
        cur.execute("SELECT CURRENT_DATE")
        data_atual = cur.fetchone()[0]

        # Calcular a data do periodo solicitado
        data_limite = data_atual - timedelta(days=7)

        # Limpar os últimos dois meses de cada tabela fato
        for tabela, coluna_data in tabelas_fato:
            query = f"""
                DELETE FROM {tabela}
                WHERE {coluna_data} >= %s
            """
            cur.execute(query, (data_limite,))

        conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def transferir_dados_incrementais(tabela,tabela_fato, data_parametro_mongo, data_parametro_fato):
    # Conexão com MongoDB
    cliente_mongo = MongoClient(os.getenv("MONGO_URI"))
    db_mongo = cliente_mongo[os.getenv("MONGO_DB")]
    colecao = db_mongo[tabela]

    # Conexão com PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
    cur = conn.cursor()

    # Verifica se a tabela fato existe
    cur.execute(f"SELECT to_regclass('{tabela_fato}')")
    tabela_existe = cur.fetchone()[0] is not None

    if not tabela_existe:
        # Se a tabela não existe, cria a tabela
        dados = list(colecao.find().limit(1))
        if dados:
            df_schema = pd.DataFrame(dados)
            if '_id' in df_schema.columns:
                df_schema = df_schema.drop(columns=['_id'])
            colunas = ', '.join([f"{col} TEXT" for col in df_schema.columns])
            cur.execute(f"CREATE TABLE {tabela} ({colunas})")
        ultima_data = None
    else:
        # Se a tabela existe, obtém a última data
        cur.execute(f"SELECT MAX({data_parametro_fato}) FROM {tabela_fato}")
        ultima_data = cur.fetchone()[0]

    if ultima_data:
        dados = list(colecao.find({data_parametro_mongo: {"$gt": ultima_data}}, {'_id': 0}))
    else:
        dados = list(colecao.find({}, {'_id': 0}))

    if dados:
        df = pd.DataFrame(dados)
        df = df.astype(str)
        valores = [tuple(x) for x in df.to_numpy()]
        colunas = ', '.join(df.columns)
        execute_values(cur, f"INSERT INTO {tabela} ({colunas}) VALUES %s", valores)

    conn.commit()
    cur.close()
    conn.close()
    cliente_mongo.close()

    print(f"Transferência incremental concluída para a tabela {tabela}")

def transferir_dados_replace(tabela):
    # Conexão com MongoDB
    cliente_mongo = MongoClient(os.getenv("MONGO_URI"))
    db_mongo = cliente_mongo[os.getenv("MONGO_DB")]
    colecao = db_mongo[tabela]

    # Obter dados do MongoDB
    dados = list(colecao.find({}, {'_id': 0}))

    if not dados:
        print(f"Nenhum dado encontrado na tabela {tabela}")
        return

    # Converter para DataFrame
    df = pd.DataFrame(dados)
    df = df.astype(str)

    # Conexão com PostgreSQL
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT")
    )
    cur = conn.cursor()

    # Criar tabela no PostgreSQL se não existir
    colunas = ', '.join([f"{col} TEXT" for col in df.columns])
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {tabela} ({colunas})
    """)

    # Limpar os dados existentes na tabela
    cur.execute(f"DELETE FROM {tabela}")

    # Inserir os novos dados
    valores = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, f"INSERT INTO {tabela} ({', '.join(df.columns)}) VALUES %s", valores)

    # Commit e fechar conexões
    conn.commit()
    cur.close()
    conn.close()
    cliente_mongo.close()

    print(f"Transferência completa concluída para a tabela {tabela}")


#DBT Configs
CONNECTION_ID = "postgres_conn"
DB_NAME = "postgres"
SCHEMA_NAME = "dw_comercial"
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/energizetech_comercial"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="energizetech_comercial",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

# Criando a DAG
dag = DAG(
    'etl_prod',
    default_args=default_args,
    description='DAG para transferir dados do MongoDB para o PostgreSQL',
    schedule_interval='09 23 * * *',
    catchup=False,
)

truncar_todas_tabelas_staging = PythonOperator(
    task_id='truncar_todas_tabelas_schema_public',
    python_callable=truncar_todas_tabelas_schema_public,
    dag=dag,
)

limpar_dados_tabelas_fatos = PythonOperator(
    task_id='limpar_ultimos_dois_meses_das_tabelas_fatos',
    python_callable=limpar_dados_fatos,
    dag=dag,
)

# Definindo as tarefas individualmente
transferir_sales = PythonOperator(
    task_id='transferir_sales',
    python_callable=transferir_dados_incrementais,
    op_kwargs={'tabela': 'sales', 'tabela_fato': 'dw_comercial.fato_vendas', 'data_parametro_mongo': 'sale_date', 'data_parametro_fato': 'data_venda'},
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

transferir_transactions = PythonOperator(
    task_id='transferir_transactions',
    python_callable=transferir_dados_incrementais,
    op_kwargs={'tabela': 'transactions', 'tabela_fato': 'dw_comercial.fato_vendas', 'data_parametro_mongo': 'transaction_date', 'data_parametro_fato': 'data_venda'},
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

transferir_returns = PythonOperator(
    task_id='transferir_returns',
    python_callable=transferir_dados_incrementais,
    op_kwargs={'tabela': 'returns', 'tabela_fato': 'dw_comercial.fato_devolucoes', 'data_parametro_mongo': 'return_date', 'data_parametro_fato': 'data_devolucao'},
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

transferir_customers = PythonOperator(
    task_id='transferir_customers',
    python_callable=transferir_dados_replace,
    op_kwargs={'tabela': 'customers'},
    dag=dag,
)

transferir_products = PythonOperator(
    task_id='transferir_products',
    python_callable=transferir_dados_replace,
    op_kwargs={'tabela': 'products'},
    dag=dag,
)

transferir_inventory = PythonOperator(
    task_id='transferir_inventory',
    python_callable=transferir_dados_replace,
    op_kwargs={'tabela': 'inventory'},
    dag=dag,
)

transferir_salespersons = PythonOperator(
    task_id='transferir_salespersons',
    python_callable=transferir_dados_replace,
    op_kwargs={'tabela': 'salespersons'},
    dag=dag,
)

transform_data = DbtTaskGroup(
    group_id="transform_data",
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=execution_config,
    default_args={"retries": 2},
    dag=dag,
)

truncar_todas_tabelas_staging >> limpar_dados_tabelas_fatos >> [
    transferir_sales,
    transferir_transactions,
    transferir_returns,
    transferir_customers,
    transferir_products,
    transferir_inventory,
    transferir_salespersons
] >> transform_data