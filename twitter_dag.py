from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime

import pandas as pd
import numpy as np
import re
from datetime import date, timedelta

default_args = {
                 "start_date": datetime(2020,1,1),
                 "owner"     : "airflow"
               }

# Here we should fetch our data from the Twitter API but since now we have to
# apply for getting API's credentials we pass this step for the sake of the tutorial.
# We use data.csv as source of tweets.

INPUT_FILE_PATH = '/home/vagrant/airflow/dags/data/data.csv'

OUTPUT_LOCAL_DIR='/tmp/output/'

def fetching_tweets():
        # Create the dataframe from data.csv
        tweets = pd.read_csv(INPUT_FILE_PATH, encoding='latin1')

        # Fomat time using pd.to_datetime and drop the column Row ID
        tweets = tweets.assign(Time=pd.to_datetime(tweets.Time)).drop('row ID', axis='columns')

        # Export the dataframe into a new csv file with the current date
        tweets.to_csv(OUTPUT_LOCAL_DIR + 'data_fetched.csv', index=False)



# This script cleans the fetched tweets from the previous task "fetching_tweets"

def cleaning_tweets():
    # Read the csv produced by the "fetching_tweets" task
    tweets = pd.read_csv(OUTPUT_LOCAL_DIR + 'data_fetched.csv')

    # Rename the columns of the dataframe
    tweets.rename(columns={'Tweet': 'tweet', 'Time':'dt', 'Retweet from': 'retweet_from', 'User':'tweet_user'}, inplace=True)

    # Drop the useless column "User" since all the tweets are written by Elon Musk
    tweets.drop(['tweet_user'], axis=1, inplace=True)

    # Add a column before_clean_len to know the size of the tweets before cleaning
    tweets['before_clean_len'] = [len(t) for t in tweets.tweet]

    # Remove @mention in tweets
    tweets['tweet'] = tweets['tweet'].apply(lambda tweet: re.sub(r'@[A-Za-z0-9]+', '', tweet))

    # Remove URL in tweets
    tweets['tweet'] = tweets['tweet'].apply(lambda tweet: re.sub('https?://[A-Za-z0-9./]+', '', tweet))

    # Remove all non letter charaters including numbers from the tweets
    tweets['tweet'] = tweets['tweet'].apply(lambda tweet: re.sub('[^a-zA-Z]', ' ', tweet))

    # Lower case all the tweets
    tweets['tweet'] = tweets['tweet'].str.lower()

    # Add after clean len column
    tweets['after_clean_len'] = [len(t) for t in tweets.tweet]

    # Changing date format
    yesterday = date.today() - timedelta(days=1)
    dt = yesterday.strftime("%Y-%m-%d")

    tweets['dt'] = dt

    # Export cleaned dataframe
    tweets.to_csv(OUTPUT_LOCAL_DIR + 'data_cleaned.csv', index=False)


# This script stores the fetched tweets from the previous task "cleaning_tweets"

def storing_tweets():
    # Read the csv produced by the "fetching_tweets" task
    tweets = pd.read_csv(OUTPUT_LOCAL_DIR + 'data_cleaned.csv')
    df = pd.DataFrame(tweets, columns= ['tweet','dt','retweet_from','before_clean_len', 'after_clean_len'])

    sql_query_list = []
    for row in df.itertuples():
        sql_query = "INSERT INTO tweets (tweet, dt, retweet_from, before_clean_len, after_clean_len) VALUES ('" + str(row.tweet) + "', '" + str(row.dt) + "', '" + str(row.retweet_from) + "', '" + str(row.before_clean_len) + "', '" + str(row.after_clean_len) + "');"
        sql_query_list.append(sql_query)
    return sql_query_list


# This script loads the  stored tweets from the previous task "storing_tweets"

def loading_tweets():
    request = "SELECT * FROM tweets"
    mysql_hook = MySqlHook(mysql_conn_id="mysql", schema="airflow_mdb")
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    tweets = cursor.fetchall()
    for tweet in tweets:
        print(tweet)
    #return tweets


with DAG(dag_id="twitter_dag", schedule_interval="*/10 * * * *", default_args=default_args) as dag:
    waiting_for_tweets = FileSensor(task_id="waiting_for_tweets", fs_conn_id="fs_tweet", filepath="data.csv", poke_interval=5)

    fetching_tweets = PythonOperator(task_id="fetching_tweets", python_callable=fetching_tweets)
    cleaning_tweets = PythonOperator(task_id="cleaning_tweets", python_callable=cleaning_tweets)
    storing_tweets = MySqlOperator(task_id="storing_tweets", sql=storing_tweets(), mysql_conn_id='mysql', autocommit=True, database='airflow_mdb')
    loading_tweets = PythonOperator(task_id="loading_tweets", python_callable=loading_tweets)

    waiting_for_tweets >> fetching_tweets >> cleaning_tweets >> storing_tweets >> loading_tweets    