# stock-analytics-dashboard
IS3107 Project

1. Run `virtualenv env`
2. Run `source env/bin/activate`
3. Run `pip install -r requirements.txt` to install dependencies
4. Run `export AIRFLOW_HOME=$(pwd)/airflow`
5. Run `airflow db init` to initialise database
6. Run `airflow webserver --debug` to start webserver in debug mode
