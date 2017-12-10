### Spin OceanRecords example database
0. Set up postgres database on your local
1. Execute sql statements located in `manual_init.sql` (those which refers to OceaReocrds operational sysytem)
2. execute: `python spin_osdb_init.py OceanRecordsInit --local-scheduler`

notes:
- make sure you've installed all necessery dependencies (requirements.txt)
- make sure that in dir when script is executed also `prod_raw` file exists
- script will output some temporary luigi related files in `tmp_init_files` folder

### Update OceanRecords database
usage: python update_osdb.py
Requires that initial state of databse already exists.

### Airflow initialization
0. Install airflow and any other prerequisite libraries
1. Set up arflow home dir, e.g.: `export AIRFLOW_HOME=~/airflow`
2. Create/edit `airflow.cfg` file. The first time you run Airflow (e.g. by running `airflow` command), it will create a config file in $AIRFLOW_HOME. Set `load_examples = False`, so example dags are not loaded.
3. In `airflow.cfg` change executor type to: LocalExecutor
4. In ``airflow.cfg`` modify sqlalchemy connection string to postgres airflow db
5. Init ariflow database: `airflow initdb`
6. Copy dags and etl_sql_code folders into AIRFLOW_HOME
7. Run airflow by: `bash start_airflow.sh &> output.log`

notes:
- all related to DWH scripts in `manual_init.sql` have to be executed before
- make sure that all paths are correct. for example I've hardcoded mine local paths in start_airflow.sh