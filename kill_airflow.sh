# kill any airflow processes (webserver, scheduler and workers)
ps -fade | grep airflow | awk '{print $2}' | xargs kill -9