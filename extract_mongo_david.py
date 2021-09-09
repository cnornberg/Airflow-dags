# [START import_module]
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# [END import_module]

# # [START MongoDB Connector]
# client = pymongo.MongoClient('mongodb://root:VQLnZB1QIp%@mongodb.airflow.svc.cluster.local:27017')
# db = client.mongo
# # [END MongoDB Connector]

# [START Query MongoDB Data Collection Produtos]
def query_mongo_collection(**context):
    import pymongo
    import json
    import pandas as pd
    from pandas.io.json import json_normalize

# [START MongoDB Connector]
    #client = pymongo.MongoClient('mongodb://root:VQLnZB1QIp@mongodb.airflow.svc.cluster.local:27017')
    #db = client.mongo
    client = pymongo.MongoClient('mongodb://root:CPcw3cgIir@mongodb.airflow.svc.cluster.local:27017')
    db = client.teste

# [END MongoDB Connector]

    #for x in db["produtos"].find():
    for x in db["testemongo"].find():
        # df = pd.json_normalize(x)
        df = str(x)
    print(df)
    # print(df.head())
    # ti.xcom_push(key='dfCollection', value=df)
    context["tasks_instance"].xcom_push(key="dfcol", value=df)
    #return df
# [END Query MongoDB Data Collection Produtos]

# [START Extract MongoDB Data]
def extract_mongo(**context):
    import pymongo
    import json
    import pandas as pd
    from pandas.io.json import json_normalize

    # ti.xcom_pull(key='dfCollection', task_ids=['query_mongo_task'])
    df = context["tasks_instance"].xcom_pull(task_ids="query_mongo_task", key="dfcol")
    print(df)
    df.to_csv('/tmp/mongo.csv')
    print("Extração Finalizada.")
    return 'Extract mongoDB completed.'
# [END Extract MongoDB Data]

# [START default_args]
default_args = {
    'owner': 'david fachini',
    'depends_on_past': False,
    'email': ['david.fachini@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'mongodb_dag_david',
    default_args=default_args,
    description='Query and Export MongoDB Data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['mongodb', 'query', 'extraction'],
) as dag:
# [END instantiate_dag]

# [START basic_task]
    query_mongo_task = PythonVirtualenvOperator(
        task_id='query_mongo_task',
        python_callable=query_mongo_collection,
        requirements=["pymongo"],
        provide_context=True,
        # ti.xcom_push=True,
    )

    extract_mongo_task = PythonVirtualenvOperator(
        task_id='extract_mongo_task',
        python_callable=extract_mongo,
        requirements=["pymongo"],
        provide_context=True,
    )

    list_csv_file_task = BashOperator(
        task_id='list_csv_file_task',
        bash_command='cat /tmp/mongo.csv',
    )
# [END basic_task]

query_mongo_task >> extract_mongo_task >> list_csv_file_task