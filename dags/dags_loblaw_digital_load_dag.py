#****************************************************
# STEP 1 - To import library needed for the operation
#*****************************************************

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from datetime import datetime, timedelta


#****************************************************
# STEP 2 - # Config variables
#*****************************************************

airflow_var = Variable.get('loblaw_load_var', deserialize_json=True)
internal_email = airflow_var.get('internal_email')



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

AIRFLOW_DAG_ID = 'loblaw_digital_load_data_dag'
AIRFLOW_DAG_DESC = 'DAG for data setup'

Dag = DAG(
    dag_id=AIRFLOW_DAG_ID,
    default_args=default_args,
    description=AIRFLOW_DAG_DESC,
    schedule_interval='00 9 * * *',
    max_active_runs=1,
    concurrency=20,
    catchup=False
)


#****************************************************
# Step 5: write down the differnt task
#*****************************************************

start = DummyOperator(
    task_id='start',
    dag=Dag
)

end = DummyOperator(
    task_id='end',
    dag=Dag
)

for table_detail in airflow_var['BQ_TABLE_LIST']:
    destination = f"{airflow_var['BQ_PROJECT_ID']}.{airflow_var['BQ_DATASET']}.{table_detail['table_name']}"

    config = {
        "task_id": f"load_{table_detail['table_name']}",
        "bucket": airflow_var['BUCKET'],
        "source_objects": table_detail['file_path'],
        "field_delimiter": table_detail.get('field_delimiter', ','),
        "source_format": table_detail.get('source_format', 'CSV'),
        "skip_leading_rows": table_detail.get('skip_leading_rows', 0),
        "destination_project_dataset_table": destination,
        "write_disposition": 'WRITE_TRUNCATE',
        "dag": Dag
    }

    if table_detail.get('table_detail', False):
        config["schema_fields"] = table_detail.get('schema_fields', None)
    else:
        config["autodetect"] = table_detail.get('autodetect', None)

    table_load = GoogleCloudStorageToBigQueryOperator(**config)

    start >> table_load >> end
