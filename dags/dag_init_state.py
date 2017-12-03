"""
This DAG should not be scheduled. It suppose to be run once at DWH creation. It spins all
tables in work, dwh and etl_control schemas.
"""
# built-in
import datetime
import os

# 3rd party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

python_bin = os.path.join(os.environ['PYTHON_DIR'],'python')
python_scripts_dir = os.environ['PYTHON_SCRIPTS_DIR']
sql_scripts_dir = os.environ['SQL_SCRIPTS_DIR']
exec_sql_cmd = '{} {}etl_handler.py --sql_script_path={}'.format(python_bin, python_scripts_dir, sql_scripts_dir)

task_default_args = {'owner': 'ETL', 'depends_on_past': False, 'priority_weight': 3}
dag = DAG('DWH-init-state', description='Recreate DWH tables from work, dwh and etl_control schemas',
          start_date=datetime.datetime(2017, 10, 23, 7, 0), catchup=False, schedule_interval=None, 
          concurrency=1, max_active_runs=1, default_args=task_default_args)

init_start = DummyOperator(task_id='START-INIT', dag=dag, trigger_rule='all_success')

spin_address = BashOperator(task_id='SPIN-address', trigger_rule='all_done', dag=dag,
                            bash_command = exec_sql_cmd + 'd_address/00_create_d_address_tables.sql')
spin_customer = BashOperator(task_id='SPIN-customer', trigger_rule='all_done', dag=dag,
                             bash_command = exec_sql_cmd + 'd_customer/00_create_d_customer_tables.sql')
spin_payment = BashOperator(task_id='SPIN-payment', trigger_rule='all_done', dag=dag,
                            bash_command = exec_sql_cmd + 'd_payment/00_create_d_payment_table.sql')
spin_product = BashOperator(task_id='SPIN-product', trigger_rule='all_done', dag=dag,
                            bash_command = exec_sql_cmd + 'd_product/00_create_d_product_table.sql')
spin_status = BashOperator(task_id='SPIN-status', trigger_rule='all_done', dag=dag,
                           bash_command = exec_sql_cmd + 'd_status/00_create_d_status_table.sql')
spin_supplier = BashOperator(task_id='SPIN-supplier', trigger_rule='all_done', dag=dag,
                             bash_command = exec_sql_cmd + 'd_supplier/00_create_d_supplier_table.sql')
spin_date = BashOperator(task_id='SPIN-date', trigger_rule='all_done', dag=dag,
                         bash_command = exec_sql_cmd + 'd_date/00_create_d_date_table.sql')
spin_order = BashOperator(task_id='SPIN-order', trigger_rule='all_done', dag=dag,
                          bash_command = exec_sql_cmd + 'f_order/00_create_f_order_tables.sql')
spin_order_line = BashOperator(task_id='SPIN-order_line', trigger_rule='all_done', dag=dag,
                               bash_command = exec_sql_cmd + 'f_order_line/00_create_f_order_line_tables.sql')
spin_queue = BashOperator(task_id='SPIN-reproc_queue', trigger_rule='all_done', dag=dag,
                          bash_command = exec_sql_cmd + 'reproc_queue/00_create_reproc_queue_table.sql')
spin_watermark = BashOperator(task_id='SPIN-watermark', trigger_rule='all_done', dag=dag,
                              bash_command = exec_sql_cmd + 'watermark/00_create_control_watermark_table.sql')

init_end = DummyOperator(task_id = 'END-INIT', dag=dag, trigger_rule='all_done')

init_start >> spin_address >> spin_queue
init_start >> spin_customer >> spin_queue
init_start >> spin_payment >> spin_queue
init_start >> spin_product >> spin_queue
init_start >> spin_status >> spin_queue
init_start >> spin_supplier >> spin_queue
init_start >> spin_date >> spin_queue
spin_queue >> spin_watermark >> spin_order >> spin_order_line
spin_order_line >> init_end