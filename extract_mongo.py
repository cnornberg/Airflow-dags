
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# [END import_module]

# Função para conectar no Mongo e consultar Collection - Extract
def extract_mongo():
    #!pip install pymongo
    import pymongo
    import json
    import pandas as pd
    from pandas.io.json import json_normalize

    # from pprint import pprint

    # Conectar no MongoDB - Database teste
    client = pymongo.MongoClient('mongodb://root:CPcw3cgIir@mongodb.airflow.svc.cluster.local:27017')
    db = client.teste

    # Consultar Collection TesteCollection:
    df = pd.json_normalize(db["TesteCollection"].find())
    
    print(df.head())

    # Salvar csv:
    df.to_csv('/tmp/teste.csv')

    print("Extração Concluída")
    return 'Fim Extract MongoDB!!!'

# 


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'mongo_dag',
    default_args=default_args,
    description='Exportar Dados MongoDB',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['mongodb,', 'teste_collection'],
) as dag:
    # [END instantiate_dag]

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
    #extract_mongo = PythonOperator(
    extract_mongo = PythonVirtualenvOperator(
        task_id='extrair_mongodb_id',
        python_callable=extract_mongo,
        requirements=["pymongo"],
    )

    cat_arquivo = BashOperator(
        task_id = 'cat_arquivo_csv',
        bash_command='cat /tmp/teste.csv',
    )

extract_mongo >> cat_arquivo
