#****************************************************
# STEP 1 - To import library needed for the operation
#*****************************************************

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from datetime import datetime, timedelta


#****************************************************
# STEP 2 - # Config variables
#*****************************************************

airflow_var = Variable.get('loblaw_product_recomm_var', deserialize_json=True)
internal_email = airflow_var.get('internal_email')
run_date = datetime.now().date()
target_table_latest = f"{airflow_var['BQ_PROJECT_ID']}.{airflow_var['BQ_DATASET']}.{airflow_var['BQ_TARGET_TABLE']}"
target_table_version = f"{airflow_var['BQ_PROJECT_ID']}.{airflow_var['BQ_DATASET']}.{airflow_var['BQ_TARGET_TABLE']}_{run_date}"
SQL = Variable.get('loblaw_product_recomm_sql', deserialize_json=False)


#****************************************************
# STEP 3 - Default Argument
#*****************************************************

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': internal_email,
    'email_on_failure': True,
    'email_on_retry': False,
    'provide_context': True
}


#****************************************************
# STEP 4 - Define DAG: Set ID and assign default args and schedule interval
#*****************************************************

AIRFLOW_DAG_ID = 'product_recommendation_dag'
AIRFLOW_DAG_DESC = 'DAG for data setup'

dag = DAG(
    dag_id=AIRFLOW_DAG_ID,
    default_args=default_args,
    description=AIRFLOW_DAG_DESC,
    schedule_interval= None,
    max_active_runs=1,
    concurrency=20,
    catchup=False
)


#****************************************************
# Step 5: write down the differnt task
#*****************************************************

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

refresh_recommendation_model_latest = BigQueryOperator(
    task_id='refresh_recommendation_model_latest',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql=SQL,
    destination_dataset_table=target_table_latest,
    dag=dag)

refresh_recommendation_model_version = BigQueryOperator(
    task_id=f'refresh_recommendation_model_{run_date}',
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    sql=SQL,
    destination_dataset_table=target_table_version,
    dag=dag)

start >> [refresh_recommendation_model_latest, refresh_recommendation_model_version] >> end
