import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import requests
import pandas as pd

def sql_execution(sql_command):
    con = psycopg2.connect(user = 'airflow', password = 'airflow', host="postgres", port="5432", database = 'airflow')
    cur = con.cursor()
    cur.execute(sql_command)
    con.commit()
    con.close

def _get_information(url):
    response = requests.get(url)
    data = response.json()
    return data

def get_user(**kwargs):
    ti = kwargs['ti']
    data = _get_information('https://jsonplaceholder.typicode.com/users')
    FIELDS = ["name", "username", "email", "address.street", "address.suite", "address.city", "address.zipcode", "address.geo.lat", "address.geo.lng", "phone", "website", "company.name", "company.catchPhrase", "company.bs"]
    df = pd.json_normalize(data)
    df[FIELDS]

    ti.xcom_push(key='get_user', value=df)

def get_post(**kwargs):
    ti = kwargs['ti']
    data = _get_information('https://jsonplaceholder.typicode.com/posts')

    ti.xcom_push(key='get_post', value=data)

def get_comment(**kwargs):
    ti = kwargs['ti']
    data = _get_information('https://jsonplaceholder.typicode.com/comments')
    
    ti.xcom_push(key='get_comment', value=data)

def get_album(**kwargs):
    ti = kwargs['ti']
    data = _get_information('https://jsonplaceholder.typicode.com/albums')
    
    ti.xcom_push(key='get_album', value=data)

def get_photo(**kwargs):
    ti = kwargs['ti']
    data = _get_information('https://jsonplaceholder.typicode.com/photos')
    
    ti.xcom_push(key='get_photo', value=data)

def branch_table_func(**kwargs):
    con = psycopg2.connect(user = 'airflow', password = 'airflow', host="postgres", port="5432", database = 'airflow')
    cur = con.cursor()

    table = kwargs['table_name'].split('_')[0]

    check_table = f"""SELECT to_regclass('{kwargs['table_name']}');"""

    cur.execute(check_table)
    log = cur.fetchone()[0]
    con.commit()
    con.close
    if str(log)=='None':
        return f'create_{table}_task'
    else:
        return f'add_{table}_task'

def create_user_task(**kwargs):

    sql_execution("""CREATE TABLE IF NOT EXISTS user_table
                    (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR,
                        username VARCHAR,
                        email VARCHAR,
                        street VARCHAR,
                        suite VARCHAR,
                        city VARCHAR,
                        zipcode VARCHAR,
                        lat VARCHAR,
                        lng VARCHAR,
                        phone VARCHAR,
                        website VARCHAR,
                        company_name VARCHAR,
                        catchPhrase VARCHAR,
                        bs VARCHAR
                    )
                    """)

def create_post_task(**kwargs):

    sql_execution("""CREATE TABLE IF NOT EXISTS post_table
                    (
                        id SERIAL PRIMARY KEY,
                        userId INTEGER,
                        title VARCHAR,
                        body VARCHAR
                    )
                    """)

def create_comment_task(**kwargs):

    sql_execution("""CREATE TABLE IF NOT EXISTS comment_table
                    (
                        id SERIAL PRIMARY KEY,
                        postId INTEGER,
                        name VARCHAR,
                        email VARCHAR,
                        body VARCHAR
                    )
                    """)

def create_album_task(**kwargs):

    sql_execution("""CREATE TABLE IF NOT EXISTS album_table
                    (
                        id SERIAL PRIMARY KEY,
                        userId INTEGER,
                        title VARCHAR
                    )
                    """)

def create_photo_task(**kwargs):

    sql_execution("""CREATE TABLE IF NOT EXISTS photo_table
                    (
                        id SERIAL PRIMARY KEY,
                        albumId INTEGER,
                        title VARCHAR,
                        url VARCHAR,
                        thumbnailUrl VARCHAR
                    )
                    """)

def add_user_task(**kwargs):
    ti = kwargs['ti']
    data=ti.xcom_pull(key='get_user')

    for ind in data.index:
        sql_execution(f"""INSERT INTO user_table
                (name, username, email, street, suite, city, zipcode, lat, lng, phone, website, company_name, catchPhrase, bs) 
                VALUES ('{data['name'][ind]}', '{data['username'][ind]}', '{data['email'][ind]}', '{data['address.street'][ind]}', 
                '{data['address.suite'][ind]}', '{data['address.city'][ind]}', '{data['address.zipcode'][ind]}', '{data['address.geo.lat'][ind]}', 
                '{data['address.geo.lng'][ind]}', '{data['phone'][ind]}', '{data['website'][ind]}', '{data['company.name'][ind]}', 
                '{data['company.catchPhrase'][ind]}', '{data['company.bs'][ind]}');""")

def add_post_task(**kwargs):
    ti = kwargs['ti']
    data=ti.xcom_pull(key='get_post')
    excluded_user_id_list = kwargs['excluded_user_id']

    for post in data:
        if post['userId'] not in excluded_user_id_list:
            sql_execution(f"""INSERT INTO post_table 
                    (userId, title, body) 
                    VALUES ('{post['userId']}', '{post['title']}', '{post['body']}');""")

def add_comment_task(**kwargs):
    ti = kwargs['ti']
    data=ti.xcom_pull(key='get_comment')
    excluded_post_id_list = kwargs['excluded_post_id']

    for comment in data:
        if comment['postId'] not in excluded_post_id_list:
            sql_execution(f"""INSERT INTO comment_table 
                    (postId, name, email, body) 
                    VALUES ('{comment['postId']}', '{comment['name']}', '{comment['email']}', '{comment['body']}');""")

def add_album_task(**kwargs):
    ti = kwargs['ti']
    data=ti.xcom_pull(key='get_album')
    excluded_user_id_list = kwargs['excluded_user_id']

    for album in data:
        if album['userId'] not in excluded_user_id_list:
            sql_execution(f"""INSERT INTO album_table 
                    (userId, title) 
                    VALUES ('{album['userId']}', '{album['title']}');""")

def add_photo_task(**kwargs):
    ti = kwargs['ti']
    data=ti.xcom_pull(key='get_photo')
    excluded_album_id_list = kwargs['excluded_album_id']

    for photo in data:
        if photo['albumId'] not in excluded_album_id_list:
            sql_execution(f"""INSERT INTO photo_table 
                    (albumId, title, url, thumbnailUrl) 
                    VALUES ('{photo['albumId']}', '{photo['title']}', '{photo['url']}', '{photo['thumbnailUrl']}');""")

default_args = {
    'owner': 'dataength',
    'start_date': datetime(2020, 7, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=2),
}

with DAG('get_information',
         schedule_interval="*/45 * * * *",
         default_args=default_args,
         description='Get information',
         catchup=False) as dag:

    user = PythonOperator(
        task_id='get_user',
        python_callable=get_user,
        provide_context=True,
        do_xcom_push=False
    )

    post = PythonOperator(
        task_id='get_post',
        python_callable=get_post,
        provide_context=True,
        do_xcom_push=False
    )

    comment = PythonOperator(
        task_id='get_comment',
        python_callable=get_comment,
        provide_context=True,
        do_xcom_push=False
    )

    album = PythonOperator(
        task_id='get_album',
        python_callable=get_album,
        provide_context=True,
        do_xcom_push=False
    )

    photo = PythonOperator(
        task_id='get_photo',
        python_callable=get_photo,
        provide_context=True,
        do_xcom_push=False
    )

    branch_user = BranchPythonOperator(
        task_id='branch_task_user',
        python_callable=branch_table_func,
        provide_context=True,
        op_kwargs={'table_name': 'user_table'}
    )

    branch_post = BranchPythonOperator(
        task_id='branch_task_post',
        python_callable=branch_table_func,
        provide_context=True,
        op_kwargs={'table_name': 'post_table'}
    )

    branch_comment = BranchPythonOperator(
        task_id='branch_task_comment',
        python_callable=branch_table_func,
        provide_context=True,
        op_kwargs={'table_name': 'comment_table'}
    )

    branch_album = BranchPythonOperator(
        task_id='branch_task_album',
        python_callable=branch_table_func,
        provide_context=True,
        op_kwargs={'table_name': 'album_table'}
    )

    branch_photo = BranchPythonOperator(
        task_id='branch_task_photo',
        python_callable=branch_table_func,
        provide_context=True,
        op_kwargs={'table_name': 'photo_table'}
    )

    create_user = PythonOperator(
        task_id='create_user_task',
        python_callable=create_user_task,
        provide_context=True
    )

    create_post = PythonOperator(
        task_id='create_post_task',
        python_callable=create_post_task,
        provide_context=True
    )

    create_comment = PythonOperator(
        task_id='create_comment_task',
        python_callable=create_comment_task,
        provide_context=True
    )

    create_album = PythonOperator(
        task_id='create_album_task',
        python_callable=create_album_task,
        provide_context=True
    )

    create_photo = PythonOperator(
        task_id='create_photo_task',
        python_callable=create_photo_task,
        provide_context=True
    )

    add_user = PythonOperator(
        task_id='add_user_task',
        python_callable=add_user_task,
        provide_context=True,
        trigger_rule='one_success'
    )

    add_post = PythonOperator(
        task_id='add_post_task',
        python_callable=add_post_task,
        provide_context=True,
        trigger_rule='one_success',
        op_kwargs={'excluded_user_id': [2,4,6]}
    )

    add_comment = PythonOperator(
        task_id='add_comment_task',
        python_callable=add_comment_task,
        provide_context=True,
        trigger_rule='one_success',
        op_kwargs={'excluded_post_id': [2,4,6]}
    )

    add_album = PythonOperator(
        task_id='add_album_task',
        python_callable=add_album_task,
        provide_context=True,
        trigger_rule='one_success',
        op_kwargs={'excluded_user_id': [2,4,6]}
    )

    add_photo = PythonOperator(
        task_id='add_photo_task',
        python_callable=add_photo_task,
        provide_context=True,
        trigger_rule='one_success',
        op_kwargs={'excluded_album_id': [2,4,6]}
    )

    user >> branch_user >> [add_user, create_user]
    post >> branch_post >> [add_post, create_post]
    comment >> branch_comment >> [add_comment, create_comment]
    album >> branch_album >> [add_album, create_album]
    photo >> branch_photo >> [add_photo, create_photo]
    create_user >> add_user
    create_post >> add_post
    create_comment >> add_comment
    create_album >> add_album
    create_photo >> add_photo