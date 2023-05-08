from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pandahouse as ph

def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c' 
                      }   

default_args = {
    'owner': 'd-jarusov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 14),
}

schedule_interval = '0 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sa_yarusov():

    @task()
    def extract_feed_actions():
        query = """SELECT user_id, toDate(time) event_date, 
                gender, age, os, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230220.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY toDate(time), user_id, gender, age, os
                format TSVWithNames"""
        df_cube_feed = ch_get_df(query=query)
        return df_cube_feed

    @task()
    def extract_message_actions():
        query =  """SELECT user_id, event_date, gender, age, os, messages_received, messages_sent, users_received, users_sent
                FROM 
                    (SELECT user_id, toDate(time) event_date,
                            gender, age, os, 
                            COUNT() messages_sent,
                            COUNT(DISTINCT reciever_id) users_sent
                    FROM simulator_20230220.message_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY toDate(time), user_id, gender, age, os) t1
                FULL JOIN 
                    (SELECT reciever_id as user_id, toDate(time) event_date,
                            COUNT() messages_received, 
                            COUNT(DISTINCT user_id) users_received
                    FROM simulator_20230220.message_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY reciever_id, toDate(time)) t2
                USING(user_id, event_date)
                format TSVWithNames"""
        df_cube_message = ch_get_df(query=query)
        return df_cube_message
    
    @task
    def transform_merge(df_cube_feed, df_cube_message):
        df_cube_merge = df_cube_feed.merge(df_cube_message, how='outer', on = ['user_id', 'event_date', 'gender', 'age', 'os'])
        return df_cube_merge
    
    @task
    def transform_os(df_cube_merge):
        df_cube_os = df_cube_merge.groupby(['os','event_date'])\
            ['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_cube_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        df_cube_os.insert(0, 'dimension', 'os')
        df_cube_os = (df_cube_os[['dimension', 'dimension_value', 'event_date', 'likes', 'views', 'messages_received',                                                   'messages_sent', 'users_received', 'users_sent']])
        
        return df_cube_os
 
    @task
    def transform_gender(df_cube_merge):
        df_cube_gender=df_cube_merge.groupby(['gender','event_date'])\
            ['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_cube_gender.gender.astype('str')              
        df_cube_gender.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        df_cube_gender.insert(0, 'dimension', 'gender')
        df_cube_gender = (df_cube_gender[['dimension', 'dimension_value', 'event_date', 'likes', 'views', 'messages_received',                                                   'messages_sent', 'users_received', 'users_sent']])
        return df_cube_gender

    @task
    def transform_age(df_cube_merge):
        
        df_cube_age = df_cube_merge.groupby(['age', 'event_date'])\
            ['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_cube_age.age.astype('str')
        df_cube_age.rename(columns = {'age' : 'dimension_value'}, inplace = True)
        df_cube_age.insert(0, 'dimension', 'age')
        df_cube_age = (df_cube_age[['dimension', 'dimension_value', 'event_date', 'likes', 'views', 'messages_received',                                                  'messages_sent', 'users_received', 'users_sent']])
        return df_cube_age
        
    @task
    def transform_union(df_cube_os, df_cube_gender, df_cube_age):
        df_all = pd.concat([df_cube_os, df_cube_gender, df_cube_age]).reset_index()
        df_all = df_all.drop(['index'], axis=1)
        df_all = df_all.astype({'likes': 'int64', 'views': 'int64', 'messages_received': 'int64', 'messages_sent': 'int64',  
                                'users_received': 'int64', 'users_sent': 'int64' })
        df_all = (df_all[['dimension', 'dimension_value', 'event_date', 'likes', 'views', 'messages_received',                                                  'messages_sent', 'users_received', 'users_sent']])
        return df_all

    @task
    def load(df_all):
        
        query_t = """CREATE TABLE IF NOT EXISTS test.djarusov
                (dimension String,
                dimension_value String,
                event_date Date,
                likes UInt64,
                views UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64)
                ENGINE = MergeTree()
                ORDER BY event_date"""
        ph.execute(query = query_t, connection = connection)
        ph.to_clickhouse(df_all, 'djarusov', index = False, connection = connection)

        
    df_cube_feed = extract_feed_actions()
    df_cube_message = extract_message_actions()
    df_cube_merge = transform_merge(df_cube_feed, df_cube_message)
    
    df_cube_gender = transform_gender(df_cube_merge)
    df_cube_os = transform_os(df_cube_merge)
    df_cube_age = transform_age(df_cube_merge)
    
    df_all = transform_union(df_cube_gender, df_cube_os, df_cube_age)
    load(df_all)

dag_sa_yarusov = dag_sa_yarusov()
