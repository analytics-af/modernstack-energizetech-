from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from pymongo import MongoClient
import pandas as pd
from psycopg2.extras import execute_values
from pathlib import Path
import random
import string
import os
from dotenv import load_dotenv


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

client = MongoClient(os.getenv("MONGO_URI"))
db = client['EnergizeTech']  # Corrigido para usar o nome exato do banco de dados

# Função para gerar dados aleatórios
def gerar_dados_aleatorios(tipo):
    if tipo == 'nome':
        return ''.join(random.choices(string.ascii_uppercase, k=1)) + ''.join(random.choices(string.ascii_lowercase, k=7))
    elif tipo == 'email':
        return f"{gerar_dados_aleatorios('nome').lower()}@example.com"
    elif tipo == 'telefone':
        return f"+55 {random.randint(10, 99)} 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
    elif tipo == 'endereco':
        return f"Rua {gerar_dados_aleatorios('nome')}, {random.randint(1, 1000)}"
    elif tipo == 'data':
        return datetime.now() - timedelta(days=random.randint(0, 365))

# Gerar dados para cada coleção e inserir no MongoDB
def gerar_e_inserir_vendas(num_registros):
    vendas = []
    for i in range(num_registros):
        venda = {
            'sale_id': i + 1,
            'customer_id': random.randint(1, 100),
            'product_id': random.randint(1, 50),
            'sale_date': gerar_dados_aleatorios('data'),
            'quantity': random.randint(1, 10),
            'total_amount': round(random.uniform(100, 10000), 2),
            'region': random.choice(['Norte', 'Sul', 'Leste', 'Oeste', 'Centro']),
            'salesperson_id': random.randint(1, 20),
            'payment_method': random.choice(['Cartão de Crédito', 'Boleto', 'Transferência']),
            'discount': round(random.uniform(0, 0.2), 2)
        }
        vendas.append(venda)
    db.sales.insert_many(vendas)

def gerar_e_inserir_clientes(num_registros):
    clientes = []
    for i in range(num_registros):
        cliente = {
            'customer_id': i + 1,
            'name': gerar_dados_aleatorios('nome'),
            'email': gerar_dados_aleatorios('email'),
            'phone': gerar_dados_aleatorios('telefone'),
            'address': gerar_dados_aleatorios('endereco'),
            'registration_date': gerar_dados_aleatorios('data'),
            'customer_type': random.choice(['B2B', 'B2C']),
            'preferred_payment_method': random.choice(['Cartão de Crédito', 'Boleto', 'Transferência'])
        }
        clientes.append(cliente)
    db.customers.insert_many(clientes)

def gerar_e_inserir_produtos(num_registros):
    produtos = []
    for i in range(num_registros):
        produto = {
            'product_id': i + 1,
            'name': f"Bateria Modelo {random.choice(string.ascii_uppercase)}-{random.randint(100, 999)}",
            'category': random.choice(['Lítio-íon', 'Chumbo-ácido', 'Níquel-cádmio']),
            'price': round(random.uniform(500, 5000), 2),
            'stock_quantity': random.randint(10, 1000),
            'supplier': gerar_dados_aleatorios('nome'),
            'warranty_period': f"{random.randint(1, 5)} anos",
            'battery_capacity': f"{random.randint(20, 100)}kWh",
            'voltage': random.choice([12, 24, 48, 96])
        }
        produtos.append(produto)
    db.products.insert_many(produtos)

def gerar_e_inserir_vendedores(num_registros):
    vendedores = []
    for i in range(num_registros):
        vendedor = {
            'salesperson_id': i + 1,
            'name': gerar_dados_aleatorios('nome'),
            'email': gerar_dados_aleatorios('email'),
            'phone': gerar_dados_aleatorios('telefone'),
            'region': random.choice(['Norte', 'Sul', 'Leste', 'Oeste', 'Centro']),
            'hire_date': gerar_dados_aleatorios('data'),
            'total_sales': random.randint(10000, 1000000)
        }
        vendedores.append(vendedor)
    db.salespersons.insert_many(vendedores)

def gerar_e_inserir_transacoes(num_registros):
    transacoes = []
    for i in range(num_registros):
        transacao = {
            'transaction_id': i + 1,
            'sale_id': random.randint(1, 1000),
            'transaction_date': gerar_dados_aleatorios('data'),
            'payment_method': random.choice(['Cartão de Crédito', 'Boleto', 'Transferência']),
            'amount': round(random.uniform(100, 10000), 2),
            'status': random.choice(['Concluída', 'Pendente', 'Cancelada'])
        }
        transacoes.append(transacao)
    db.transactions.insert_many(transacoes)

def gerar_e_inserir_devolucoes(num_registros):
    devolucoes = []
    for i in range(num_registros):
        devolucao = {
            'return_id': i + 1,
            'sale_id': random.randint(1, 1000),
            'customer_id': random.randint(1, 100),
            'product_id': random.randint(1, 50),
            'return_date': gerar_dados_aleatorios('data'),
            'quantity_returned': random.randint(1, 5),
            'return_amount': round(random.uniform(100, 5000), 2),
            'return_reason': random.choice(['Defeito', 'Insatisfação', 'Produto errado'])
        }
        devolucoes.append(devolucao)
    db.returns.insert_many(devolucoes)

def gerar_e_inserir_estoque(num_registros):
    estoque = []
    for i in range(num_registros):
        item_estoque = {
            'inventory_id': i + 1,
            'product_id': random.randint(1, 50),
            'warehouse_id': random.randint(1, 5),
            'stock_level': random.randint(10, 1000),
            'reorder_point': random.randint(5, 50),
            'inventory_value': round(random.uniform(1000, 100000), 2),
            'last_update': gerar_dados_aleatorios('data')
        }
        estoque.append(item_estoque)
    db.inventory.insert_many(estoque)

dag = DAG(
    'insert_mongo',
    default_args=default_args,
    description='Inserir dados comercial EnergizeTech no MongoDB',
    schedule_interval='09 23 * * *',
    catchup=False,
)

gerador_vendas = PythonOperator(
    task_id='gerar_e_inserir_dados_vendas',
    python_callable=gerar_e_inserir_vendas,
    op_kwargs={'num_registros': 50000},
    dag=dag,
)

gerador_clientes = PythonOperator(
    task_id='gerar_e_inserir_dados_clientes',
    python_callable=gerar_e_inserir_clientes,
    op_kwargs={'num_registros': 100},
    dag=dag,
)

gerador_produtos = PythonOperator(
    task_id='gerar_e_inserir_dados_produtos',
    python_callable=gerar_e_inserir_produtos, 
    op_kwargs={'num_registros': 50},
    dag=dag,
)

gerador_vendedores = PythonOperator(
    task_id='gerar_e_inserir_dados_vendedores',
    python_callable=gerar_e_inserir_vendedores, 
    op_kwargs={'num_registros': 20},
    dag=dag,
)

gerador_transacoes = PythonOperator(
    task_id='gerar_e_inserir_dados_transacoes',
    python_callable=gerar_e_inserir_transacoes, 
    op_kwargs={'num_registros': 50000},
    dag=dag,
)

gerador_devolucoes = PythonOperator(
    task_id='gerar_e_inserir_dados_devolucoes',
    python_callable=gerar_e_inserir_devolucoes, 
    op_kwargs={'num_registros': 10000},
    dag=dag,
)

gerador_estoque = PythonOperator(
    task_id='gerar_e_inserir_dados_estoque',
    python_callable=gerar_e_inserir_estoque,
    op_kwargs={'num_registros': 10023},
    dag=dag,
)

gerador_vendas >> gerador_clientes >> gerador_produtos >> gerador_vendedores >> gerador_transacoes >> gerador_devolucoes >> gerador_estoque
