from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def copy_users():
    source = PostgresHook(postgres_conn_id="source_db")
    target = PostgresHook(postgres_conn_id="target_db")

    # читаем из первой базы
    rows = source.get_records("SELECT id, name FROM users")

    # пишем во вторую
    target.run("CREATE TABLE IF NOT EXISTS users_copy (id INT PRIMARY KEY, name TEXT)")
    target.insert_rows(
        table="users_copy",
        rows=rows,
        target_fields=["id", "name"],
        replace=True,
        replace_index="id",
    )
    print(f"Скопировано строк: {len(rows)}")


with DAG(
    dag_id="copy_users",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="copy_users", python_callable=copy_users)
