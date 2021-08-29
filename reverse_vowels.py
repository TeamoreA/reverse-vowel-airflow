from __future__ import print_function

import datetime

from airflow import models
from airflow.operators import python_operator

today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=1)

default_dag_args = {
    'start_date': yesterday,
}

with models.DAG(
        'composer_reverse_vowel',
        schedule_interval=datetime.timedelta(hours=1),
        default_args=default_dag_args) as dag:
    def reverse():
        import logging
        import random
        import string

        import requests

        # generates a random string text with varying length from 20 to 25
        length = random.randint(20, 25)
        message = ''.join((random.choice(string.ascii_lowercase) for x in range(length)))

        # call the vowel-service endpoint
        backend_url = 'https://c-backend-kkirjd3lsa-nw.a.run.app/vowel-service'
        response = requests.post(backend_url, data={'message': message})
        reversed_vowel = response.json()

        logging.info("=============++++++++++++++++")
        logging.info(f"Input text is <{message}>.\tOutput text is <{reversed_vowel}>")
        logging.info("=============++++++++++++++++")

    # reverse_vowel task calls the 'reverse' function.
    reverse_vowel = python_operator.PythonOperator(
        task_id='reverse-vowel',
        python_callable=reverse)

    reverse_vowel
