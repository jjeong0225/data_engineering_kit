import requests
import psycopg2

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime


dag = DAG(
    dag_id = 'dag_gender',
    start_date = datetime(2021,2,4),
    schedule_interval = '55 14 * * *'
)


def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "four2qhrm"
    redshift_pass = "42Qhrm!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=False)
    return conn


def extract(**context):
    f = requests.get("https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv")
    data = f.text
    return data


def transform(**context):
    line = context['ti'].xcom_pull(task_ids='extract').split('\n')
    lines = line[1:]
    return lines


def load(**context):
    lines = context['ti'].xcom_pull(task_ids='transform')
    conn = get_Redshift_connection()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN")
           
        cur.execute("DELETE FROM four2qhrm.name_gender;")
        
        for r in lines:
            if r != '':
                (name, gender) = r.split(",")
                sql = "INSERT INTO four2qhrm.name_gender VALUES - ('{name}', '{gender}')".format(name=name, gender=gender)
                    
                cur.execute(sql)
            
        cur.execute("END")

    except Exception as e:
        conn.rollback()
        print('ERROR : ' + str(e))
    
    finally:
        if conn:
            cur.close()
            conn.close()



extract_op = PythonOperator(
    task_id = 'extract',
    provide_context = True,
    python_callable = extract,
    dag = dag
)


transform_op = PythonOperator(
    task_id = 'transform',
    provide_context = True,
    python_callable = transform,
    dag = dag
)

load_op = PythonOperator(
    task_id = 'extract',
    provide_context = True,
    python_callable = load,
    dag = dag
)


extract_op >> transform_op >> load_op

