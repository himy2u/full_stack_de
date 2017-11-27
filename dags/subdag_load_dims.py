# 3rd party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


def load_dims(_dag_args, _sched, exec_sql_cmd):
    dag_args = {'owner': 'ETL',
                'depends_on_past': False,
                'start_date': _dag_args['start_date']}
    dag = DAG('ETL-full-pipeline.ETL-DIMS', default_args=_dag_args, schedule_interval=_sched)
    dims_start = DummyOperator(task_id='START-LOAD-DIMS', dag=dag, trigger_rule='all_success')
    xform_address = BashOperator(task_id = 'XFORM-address', trigger_rule='all_done', dag=dag,
                                 bash_command = exec_sql_cmd + 'd_address/01_end2end_load_d_address.sql')
    xform_customer = BashOperator(task_id = 'XFORM-customer', trigger_rule='all_done', dag=dag,
                                  bash_command = exec_sql_cmd + 'd_customer/01_end2end_load_d_customer.sql')
    xform_payment = BashOperator(task_id = 'XFORM-payment', trigger_rule='all_done', dag=dag,
                                 bash_command = exec_sql_cmd + 'd_payment/01_end2end_load_d_payment.sql')
    xform_status = BashOperator(task_id = 'XFORM-status', trigger_rule='all_done', dag=dag,
                                bash_command = exec_sql_cmd + 'd_status/01_end2end_load_d_status.sql')
    xform_supplier = BashOperator(task_id = 'XFORM-supplier', trigger_rule='all_done', dag=dag,
                                  bash_command = exec_sql_cmd + 'd_supplier/01_end2end_load_d_supplier.sql')
    xform_product = BashOperator(task_id = 'XFORM-product', trigger_rule='all_done', dag=dag,
                                  bash_command = exec_sql_cmd + 'd_product/01_end2end_load_d_product.sql')
    dims_end = DummyOperator(task_id='END-LOAD-DIMS', dag=dag, trigger_rule='all_done')
    dims_start >> xform_address >> dims_end
    dims_start >> xform_customer >> dims_end
    dims_start >> xform_payment >> dims_end
    dims_start >> xform_status >> dims_end
    dims_start >> xform_supplier >> dims_end
    dims_start >> xform_product >> dims_end
    return dag