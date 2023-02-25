import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago

def save_data_into_db(**kwargs):

    import logging
    import sys, traceback
    import traceback

    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info("Saving data...")

    # Pulling data from previous task instance (or "ti")
    ti = kwargs['ti']
    LOGGER.info("Sync XCom to pull data from previous task by ID...")

    data = ti.xcom_pull(task_ids='get_covid19_report_today')
    LOGGER.info("Data : {}".format(data))

    data = json.loads(data)
    print(data)
    for r in data:
        Txn_date = datetime.strptime(r["update_date"],'%Y-%m-%d %H:%M:%S') + timedelta(days=1)
        Txn_date = Txn_date.strftime('%Y-%m-%d')
        New_case = r["new_case"]
        Total_case = r["total_case"]
        Total_case_excludeabroad = r["total_case_excludeabroad"]
        New_death = r["new_death"]
        Total_death = r["total_death"]
        New_recovered = r["new_recovered"]
        Total_recovered = r["total_recovered"]
        Update_date = r["update_date"]
      
    
    print(Txn_date)
    print(New_case)

    dt1 = '2021-09-28'
    dt2 = datetime.strptime(Update_date, '%Y-%m-%d  %H:%M:%S')

    # "covid19_db" was declare in Admin > Connections via AirFlow's UI
    mysql_hook = MySqlHook(mysql_conn_id='covid19_db')
    a = mysql_hook.get_conn()
    c = a.cursor()

    insert = """
        INSERT INTO daily_covid19_reports (
            txn_date,
            new_case,
            total_case,
            total_case_excludeabroad,
            new_death,
            total_death,
            new_recovered,
            total_recovered,
            update_date)
        VALUES ('2021-09-29', 10414, 1591829, 10404, 1585939, 122, 16620, 1460005, '2021-09-28');
    """

    val = (Txn_date, New_case, Total_case, Total_case_excludeabroad, New_death, Total_death,New_recovered, Total_recovered, Update_date)
    mysql_hook.run(insert)

    c.executemany(insert, val)  
    a.commit()
	

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 1),
    'provide_context': True  # to support task instance for XComms with kwargs['ti']
}

with DAG('covid19_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for COVID-19 report',
         catchup=True) as dag:


    t1 = SimpleHttpOperator(
        task_id='get_covid19_report_today',
        method='GET',
        http_conn_id='https_covid19_api',
        endpoint='/api/Cases/today-cases-all',
        headers={"Content-Type":"application/json"},
        response_filter=lambda response: json.dumps(json.loads(response.text)),
        dag=dag
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db,
        dag=dag
    )
    
    t1 >> t2