# built-in
import datetime

# 3rd party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

# custom
from subdag_extraction import extract_backed_tables
from subdag_load_dims import load_dims

sched = datetime.timedelta(minutes=5)
start_date = datetime.datetime(2017, 10, 23, 7, 0)
catchup = False

# TODO: put to environmental variables:
extraction_dir = '/Users/stulski/Desktop/osobiste/fullstack_de/extraction'
python_bin = '/Users/stulski/py3/bin/python'
python_scripts_dir = '/Users/stulski/Desktop/osobiste/fullstack_de/'
sql_scripts_dir = '/Users/stulski/Desktop/osobiste/fullstack_de/airflow/etl_sql_code/'

extraction_cmd = '{} {}extractor.py --extraction_dir={} --table='.format(python_bin, python_scripts_dir, extraction_dir)
stage_cmd = '{} {}etl_handler.py --extraction_abs_path={} --table_to_stage='.format(python_bin, python_scripts_dir, extraction_dir)
exec_sql_cmd = '{} {}etl_handler.py --sql_script_path={}'.format(python_bin, python_scripts_dir, sql_scripts_dir)

task_default_args = {'owner': 'ETL',
                     'depends_on_past': False,
                     'max_active_runs': 6,
                     'start_date': start_date,
                     'priority_weight': 3,
                     'catchup':catchup}

dag = DAG('ETL-full-pipeline',
          description="Runs full ETL pipeline (from extract, stage, transform, load etc.)",
          start_date=start_date, catchup=catchup, schedule_interval=sched, concurrency=6,
          max_active_runs=1, default_args=task_default_args)

dflow_start = DummyOperator(task_id='START-ETL', dag=dag, trigger_rule='all_success') # dummy starting point

subdag_EXTRACT = SubDagOperator(subdag = extract_backed_tables(task_default_args, sched, extraction_cmd, stage_cmd),
                                task_id = 'ETL-EXTRACT', dag=dag, trigger_rule='all_done')

subdag_LOAD_DIMS = SubDagOperator(subdag = load_dims(task_default_args, sched, exec_sql_cmd),
                                  task_id = 'ETL-DIMS', dag=dag, trigger_rule='all_done')

dflow_end = DummyOperator(task_id = 'END-ETL', dag=dag, trigger_rule='all_done') # dummy ending point

# Set all dependencies
dflow_start >> subdag_EXTRACT >> subdag_LOAD_DIMS >> dflow_end
