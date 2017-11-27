# 3rd party
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


def extract_backed_tables(_dag_args, _sched, extraction_cmd, stage_cmd):
    dag_args = {'owner': 'ETL',
                'depends_on_past': False,
                'start_date': _dag_args['start_date']}

    dag = DAG('ETL-full-pipeline.ETL-EXTRACT', default_args=dag_args, schedule_interval=_sched)

    extract_start = DummyOperator(task_id='START-EXTRACTION', dag=dag, trigger_rule='all_success')

    extract_prodcuts = BashOperator(task_id = 'EXTRACT-os.products',
                                    bash_command = extraction_cmd +'os.products',
                                    trigger_rule='all_done', dag=dag)

    stage_prodcuts = BashOperator(task_id = 'STAGE-os.products',
                                  bash_command = stage_cmd +'os.products',
                                  trigger_rule='all_done', dag=dag)

    extract_customers = BashOperator(task_id = 'EXTRACT-os.customers',
                                     bash_command = extraction_cmd +'os.customers',
                                     trigger_rule='all_done', dag=dag)

    stage_customers = BashOperator(task_id = 'STAGE-os.customers',
                                   bash_command = stage_cmd +'os.customers',
                                   trigger_rule='all_done', dag=dag)

    extract_addresses = BashOperator(task_id = 'EXTRACT-os.addresses',
                                     bash_command = extraction_cmd +'os.addresses',
                                     trigger_rule='all_done', dag=dag)

    stage_addresses = BashOperator(task_id = 'STAGE-os.addresses',
                                   bash_command = stage_cmd +'os.addresses',
                                   trigger_rule='all_done', dag=dag)

    extract_cust_addresses = BashOperator(task_id = 'EXTRACT-os.customers_addresses',
                                          bash_command = extraction_cmd +'os.customers_addresses',
                                          trigger_rule='all_done', dag=dag)

    stage_cust_addresses = BashOperator(task_id = 'STAGE-os.customers_addresses',
                                        bash_command = stage_cmd +'os.customers_addresses',
                                        trigger_rule='all_done', dag=dag)

    extract_payment_methods = BashOperator(task_id = 'EXTRACT-os.payment_methods',
                                           bash_command = extraction_cmd +'os.payment_methods',
                                           trigger_rule='all_done', dag=dag)

    stage_payment_methods = BashOperator(task_id = 'STAGE-os.payment_methods',
                                         bash_command = stage_cmd +'os.payment_methods',
                                         trigger_rule='all_done', dag=dag)

    extract_order_status = BashOperator(task_id = 'EXTRACT-os.order_status',
                                        bash_command = extraction_cmd +'os.order_status',
                                        trigger_rule='all_done', dag=dag)

    stage_order_status = BashOperator(task_id = 'STAGE-os.order_status',
                                      bash_command = stage_cmd +'os.order_status',
                                      trigger_rule='all_done', dag=dag)

    extract_customer_orders = BashOperator(task_id = 'EXTRACT-os.customer_orders',
                                           bash_command = extraction_cmd +'os.customer_orders',
                                           trigger_rule='all_done', dag=dag)

    stage_customer_orders = BashOperator(task_id = 'STAGE-os.customer_orders',
                                         bash_command = stage_cmd +'os.customer_orders',
                                         trigger_rule='all_done', dag=dag)

    extract_product_types = BashOperator(task_id = 'EXTRACT-os.product_types',
                                         bash_command = extraction_cmd +'os.product_types',
                                         trigger_rule='all_done', dag=dag)

    stage_product_types = BashOperator(task_id = 'STAGE-os.product_types',
                                       bash_command = stage_cmd +'os.product_types',
                                       trigger_rule='all_done', dag=dag)

    extract_product_subtypes = BashOperator(task_id = 'EXTRACT-os.product_subtypes',
                                            bash_command = extraction_cmd +'os.product_subtypes',
                                            trigger_rule='all_done', dag=dag)

    stage_product_subtypes = BashOperator(task_id = 'STAGE-os.product_subtypes',
                                          bash_command = stage_cmd +'os.product_subtypes',
                                          trigger_rule='all_done', dag=dag)

    extract_suppliers = BashOperator(task_id = 'EXTRACT-os.suppliers',
                                     bash_command = extraction_cmd +'os.suppliers',
                                     trigger_rule='all_done', dag=dag)

    stage_suppliers = BashOperator(task_id = 'STAGE-os.suppliers',
                                   bash_command = stage_cmd +'os.suppliers',
                                   trigger_rule='all_done', dag=dag)

    extract_cust_orders_prods = BashOperator(task_id = 'EXTRACT-os.customer_orders_products',
                                             bash_command = extraction_cmd +'os.customer_orders_products',
                                             trigger_rule='all_done', dag=dag)

    stage_cust_orders_prods = BashOperator(task_id = 'STAGE-os.customer_orders_products',
                                           bash_command = stage_cmd +'os.customer_orders_products',
                                           trigger_rule='all_done', dag=dag)

    extract_end = DummyOperator(task_id = 'END-EXTRACTION', dag=dag, trigger_rule='all_done')

    # Set dependencies
    extract_start >> extract_prodcuts >> stage_prodcuts >> extract_end
    extract_start >> extract_customers >> stage_customers >> extract_end
    extract_start >> extract_addresses >> stage_addresses >> extract_end
    extract_start >> extract_cust_addresses >> stage_cust_addresses >> extract_end
    extract_start >> extract_payment_methods >> stage_payment_methods >> extract_end
    extract_start >> extract_order_status >> stage_order_status >> extract_end
    extract_start >> extract_customer_orders >> stage_customer_orders >> extract_end
    extract_start >> extract_product_types >> stage_product_types >> extract_end
    extract_start >> extract_product_subtypes >> stage_product_subtypes >> extract_end
    extract_start >> extract_suppliers >> stage_suppliers >> extract_end
    extract_start >> extract_cust_orders_prods >> stage_cust_orders_prods >> extract_end
    return dag
