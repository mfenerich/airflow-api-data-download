from datetime import datetime, timedelta
import logging
import os
import json
import csv
import calendar

from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="fetch_aemet_data_with_sensor",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch and process data from AEMET API with sensor",
    schedule_interval=None,  # Run on demand
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # Retrieve the base directory from Airflow Variables
    # Provide a default if it’s not set.
    base_output_dir = Variable.get("ROOT_OUTPUT_DIRECTORY", default_var="/opt/airflow/output")
    OUTPUT_DIRECTORY = os.path.join(base_output_dir, dag.dag_id)
    OUTPUT_CSV = os.path.join(OUTPUT_DIRECTORY, "aemet_data.csv")

    def get_last_day_of_month(year: int, month: int) -> int:
        """Return the last day of a given month and year."""
        _, last_day = calendar.monthrange(year, month)
        return last_day

    def fetch_data(year: int, month: int, start_date: str, end_date: str, **kwargs) -> str:
        """
        Fetch the 'datos' URL from the AEMET API.
        """
        logging.info(f"Fetching datos URL for Year: {year}, Month: {month}")

        http_hook = HttpHook(http_conn_id="aemet_api", method="GET")
        conn = http_hook.get_connection(http_hook.http_conn_id)
        api_key = conn.extra_dejson.get("api_key")

        if not api_key:
            raise ValueError("API key is missing in the connection's extra field.")

        endpoint = f"/opendata/api/antartida/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/89064"
        response = http_hook.run(endpoint=endpoint, data={"api_key": api_key})
        response.raise_for_status()

        try:
            response_data = response.json()
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to decode JSON from API response: {e}")

        datos_url = response_data.get("datos")
        if not datos_url:
            raise ValueError("The 'datos' URL is missing in the API response.")
        if not isinstance(datos_url, str) or not datos_url.startswith("http"):
            raise ValueError("The 'datos' URL is not a valid string or does not start with 'http'.")

        logging.info(f"Fetched datos URL for {year}-{month:02d}: {datos_url}")
        return datos_url

    def check_data_availability(year: int, month: int, datos_url: str, **kwargs) -> bool:
        """
        Check if data at 'datos_url' is available.
        """
        logging.info(f"Checking data availability for {year}-{month:02d}: {datos_url}")
        http_hook = HttpHook(http_conn_id="aemet_api", method="GET")
        response = http_hook.run(endpoint=datos_url)
        return response.status_code == 200

    def write_csv_headers_if_needed(record: dict, csv_path: str) -> None:
        """Write CSV headers if the file does not exist."""
        if not os.path.exists(csv_path):
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(record.keys())

    def process_data(year: int, month: int, datos_url: str, **kwargs) -> None:
        """
        Fetch and process data from the 'datos_url' JSON endpoint,
        then append the records to a CSV file.
        """
        logging.info(f"Processing data for {year}-{month:02d} from {datos_url}.")

        # Ensure the output directory exists at runtime
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)

        http_hook = HttpHook(http_conn_id=None, method="GET")
        response = http_hook.run(endpoint=datos_url)
        response.raise_for_status()

        try:
            records = response.json()
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response from {datos_url}: {e}")

        if records and not isinstance(records, list):
            raise ValueError("Expected a list of records but got something else.")

        if records and not all(isinstance(r, dict) for r in records):
            raise ValueError("All records should be dictionaries.")

        if not records:
            logging.info(f"No records found for {year}-{month:02d}.")
            return

        expected_keys = set(records[0].keys())
        for i, record in enumerate(records, start=1):
            if set(record.keys()) != expected_keys:
                logging.warning(f"Record #{i} in {year}-{month:02d} has inconsistent schema.")

        logging.info(f"Processing {len(records)} records for {year}-{month:02d}.")
        write_csv_headers_if_needed(records[0], OUTPUT_CSV)

        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            for record in records:
                writer.writerow(record.values())

        logging.info(f"Successfully processed and saved {len(records)} records for {year}-{month:02d} to {OUTPUT_CSV}.")

    # Hardcoded years
    start_year = 2018
    end_year = 2020

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            start_date = f"{year}-{month:02d}-01T00:00:00UTC"
            last_day = get_last_day_of_month(year, month)
            end_date = f"{year}-{month:02d}-{last_day}T23:59:59UTC"

            fetch_task = PythonOperator(
                task_id=f"fetch_datos_url_{year}_{month}",
                python_callable=fetch_data,
                op_kwargs={
                    "year": year,
                    "month": month,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )

            sensor_task = PythonSensor(
                task_id=f"wait_for_data_{year}_{month}",
                python_callable=check_data_availability,
                op_kwargs={
                    "year": year,
                    "month": month,
                    "datos_url": "{{ ti.xcom_pull(task_ids='fetch_datos_url_" + f"{year}_{month}" + "') }}"
                },
                timeout=60,
                poke_interval=5,
                mode="poke",
            )

            process_task = PythonOperator(
                task_id=f"process_data_{year}_{month}",
                python_callable=process_data,
                op_kwargs={
                    "year": year,
                    "month": month,
                    "datos_url": "{{ ti.xcom_pull(task_ids='fetch_datos_url_" + f"{year}_{month}" + "') }}"
                },
            )

            fetch_task >> sensor_task >> process_task
