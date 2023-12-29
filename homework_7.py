from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from dateutil.tz import tzutc
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
API_KEY = os.environ.get("API_KEY")
MY_LOCATION = "Warszawa"

API_KEY=6be8059f585a0e6c944307c7ad3f5065
def get_temperature(location: str = "Lublin") -> float:
    """Function gets location and takes tne temperature in celsius"""

    url = f"https://api.openweathermap.org/data/2.5/weather?q={location}&appid={API_KEY}"
    response = requests.get(url=url)
    kelvin = response.json()["main"]["temp"]
    return kelvin - 273.15


def choose_answer(ti, **kwargs) -> str:
    temp = ti.xcom_pull(task_ids=["get_temperature"])[0]
    print(f"Температура в локации {MY_LOCATION} - {temp}C")
    if temp >= 15:
        return "warm"
    return "cold"


with DAG(
    dag_id="check_weather",
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 1, 1, tzinfo=tzutc()),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
) as dag:

    get_temp = PythonOperator(
        task_id="get_temperature",
        python_callable=get_temperature,
        provide_context=True,
        op_kwargs={"location": MY_LOCATION}
    )

    choosing_answer = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_answer,
        provide_context=True,
    )

    warm = BashOperator(
        task_id="warm",
        bash_command="echo 'теплo'"
    )

    cold = BashOperator(
        task_id="cold",
        bash_command="echo 'холоднo'"
    )

    get_temp >> choosing_answer >> [warm, cold]
# print(get_temperature(MY_LOCATION))
