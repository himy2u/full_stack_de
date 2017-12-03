# 3rd party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


def load_facts(_dag_args, _sched, exec_sql_cmd):
    dag_args = {'owner': 'ETL',
                'depends_on_past': False,
                'start_date': _dag_args['start_date']}
    dag = DAG('ETL-full-pipeline.ETL-FACTS', default_args=_dag_args, schedule_interval=_sched)
    facts_start = DummyOperator(task_id='START-LOAD-facts', dag=dag, trigger_rule='all_success')
    update_watekmarks = BashOperator(task_id = 'UPDATE-watermarks', trigger_rule='all_done', dag=dag,
                                     bash_command = exec_sql_cmd+'watermark/01_update_watermark_with_stage_max.sql')
    xform_order = BashOperator(task_id = 'XFORM-order', trigger_rule='all_done', dag=dag,
                               bash_command = exec_sql_cmd + 'f_order/01_end2end_load_f_order.sql')
    xform_order_line = BashOperator(task_id = 'XFORM-order_line', trigger_rule='all_done', dag=dag,
                                    bash_command = exec_sql_cmd+'f_order_line/01_end2end_load_f_order_line.sql')
    facts_end = DummyOperator(task_id='END-LOAD-facts', dag=dag, trigger_rule='all_done')
    manage_reproc = BashOperator(task_id = 'UPDATE-reproc_queue', trigger_rule='all_done', dag=dag,
                                 bash_command = exec_sql_cmd+'reproc_queue/01_manage_reproc_queue.sql')
    facts_start >> update_watekmarks >> xform_order >> xform_order_line >> manage_reproc >> facts_end
    return dag