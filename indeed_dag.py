from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import indeed_crawler
import indeed_etl



def indeed_crawl():
    for job_type in ['data_scientist','data_analyst','data_engineer', 'business_analyst']:
        request_url = indeed_crawler.indeed_url('Toronto','ON', job_type)
        indeed_crawler.data_extract(request_url, job_type)
def indeed_etl():
        skill_df = indeed_etl.read_indeed_csv('/root/Projects/IndeedCrawler/indeed_raw_data.csv')
        skill_df = indeed_etl.etl_data(skill_df)
        indeed_etl.save_data_sql(skill_df)

dag = DAG('indeed_dag', description='DAG for indeed crawler and etl',
          schedule_interval='0 0 * * *',
          start_date=datetime(2018, 3, 20), catchup=False)

start_operator = DummyOperator(task_id='start_task', retries=3, dag=dag)

crawl_operator = PythonOperator(task_id='crawl_task', python_callable=indeed_crawl, dag=dag)

etl_operator = PythonOperator(task_id='etl_task', python_callable=indeed_etl, dag=dag)

start_operator >> crawl_operator >> etl_operator
