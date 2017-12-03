# start airflow server, scheduler and workers locally
# usage: bash start_airflow.sh &> output.log

export PYTHON_DIR='/Users/stulski/py3/bin'
export AIRFLOW_HOME='/Users/stulski/Desktop/osobiste/fullstack_de/airflow'
export NO_WORKERS=6
# TODO(stulski): move variables from airflow code here
export EXTRACTION_DIR='/Users/stulski/Desktop/osobiste/fullstack_de/extraction'
export PYTHON_SCRIPTS_DIR='/Users/stulski/Desktop/osobiste/fullstack_de/'
export SQL_SCRIPTS_DIR='/Users/stulski/Desktop/osobiste/fullstack_de/airflow/etl_sql_code/'

# start UI
$PYTHON_DIR/airflow webserver -p 8080 &

# start scheduler
$PYTHON_DIR/airflow scheduler &

# start workers
for run in {1..$NO_WORKERS}
do
  $PYTHON_DIR/airflow worker &
done