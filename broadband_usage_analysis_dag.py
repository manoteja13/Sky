import airflow
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.dataflow_operator import DataflowStartFlexTemplateOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import storage
from google.cloud.storage import Blob


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('load_csv_files_into_GCS_DAG', schedule_interval="15 11 * * *",
          description='load Broadband Usage CSV files into BigQuery', default_args=default_dag_args)
ts_fd = datetime.today().strftime('%Y%m%d%H%M')


def check_file(**kwargs):
    client = storage.Client()
    bucket = client.bucket('broadband-usage')
    blobs = bucket.list_blobs(prefix='/data/input/)
    num = 0
    for blob in blobs:
        print('********* ' + blob.name)
        num += 1
    if num == 0:
        return 'skip_and_exit'
    elif num == 2:
        return 'get_file_name'
    else:
        return 'Unprocessed'

file_watcher = BranchPythonOperator(
    task_id='file_watcher',
    python_callable=check_file,
    dag=dag,
    provide_context=True
)

skip_and_exit = BashOperator(
    task_id='skip_and_exit',
    bash_command='echo "No File Exists"; exit 0;',
    dag=dag
)


def get_file_name(**kwargs):
    client = storage.Client()
    bucket = client.bucket('broadband-usage')
    blobs = bucket.list_blobs(prefix='data/input/')
    for blob in blobs:
        if 'UL' in blob.name:
            h1 = blob.name
        elif 'DL' in blob.name:
            h2 = blob.name
    return h1, h2


get_file_name = PythonOperator(
    task_id='get_file_name',
    python_callable=check_file,
    dag=dag,
    provide_context=True
)

Unprocessed = GCSToGCSOperator(
    task_id='Unprocessed',
    source_bucket='data',
    source_object='sky/data/input/*.csv',
    destination_bucket='sky/data/unprocessed/' + ts_fd + '/',
    move_object=True,
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)

start_UL_dataflow_runner = DataflowStartFlexTemplateOperator(
    task_id='start_UL_dataflow_runner',
    containerSpecGcsPath='< Template path>',
    job_name='batch-load_csv_files_into_GCS_bucket-daily',
    parameters={
        'project': "<<project_name>>",
        'region': 'us-central',
        'staging_location': "gs://<...>/temp/",
        'temp_location': "gs://<...>/temp/",
        'runner': "DataflowRunner",
        'input_path': {{ti.xcom_pull("read_file_name.h1")}},
        'output': "gs://<...>/output/"
    },
    dag=dag
)

start_DL_dataflow_runner = DataflowStartFlexTemplateOperator(
    task_id='start_DL_dataflow_runner',
    containerSpecGcsPath='< Template path>',
    job_name='batch-load_csv_files_into_GCS_bucket-daily',
    parameters={
        'project': "<<project_name>>",
        'region': 'us-central',
        'staging_location': "gs://<...>/temp/",
        'temp_location': "gs://<...>/temp/",
        'runner': "DataflowRunner",
        'input_path': {{ti.xcom_pull("read_file_name.h2")}},
        'output': "gs://<...>/output/"
    },
    dag=dag
)

Archive = GCSToGCSOperator(
    task_id='Archive',
    source_bucket='data',
    source_object='sky/data/input/*.csv',
    destination_bucket='sky/data/archive/' + ts_fd + '/',
    move_object=True,
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag

)

file_watcher >> get_file_name >> start_UL_dataflow_runner >> start_DL_dataflow_runner >> Archive
file_watcher >> Unprocessed
file_watcher >> skip_and_exit
