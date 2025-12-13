from google.cloud import bigquery
import pandas as pd
import os   

def get_latest_staging_folder(path:str) -> str:
    folders = [
        f for f in os.listdir(path)
        if os.path.isdir(os.path.join(path, f))
    ]
    folders.sort(reverse=True)
    return os.path.join(path, folders[0])

def upload_csv_to_bigquery(df: pd.DataFrame, table_id: str):
    client = bigquery.Client(project="people-analytics-etl")
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Uploaded to bigquery: {table_id}")

def run_load_bigquery():
    project = "people-analytics-etl"
    dataset = "people_analytics"

    emp_folder = get_latest_staging_folder("data/staging/employee")
    emp_path = os.path.join(emp_folder, "employee_cleaned.csv")
    df_emp = pd.read_csv(emp_path)
    upload_csv_to_bigquery(df_emp, f"{project}.{dataset}.dim_employee")

    att_folder = get_latest_staging_folder("data/staging/attendance")
    att_path = os.path.join(att_folder, "attendance_cleaned.csv")
    df_att = pd.read_csv(att_path)
    upload_csv_to_bigquery(df_att, f"{project}.{dataset}.fact_attendance")

    eng_folder = get_latest_staging_folder("data/staging/engagement")
    eng_path = os.path.join(eng_folder, "engagement_cleaned.csv")
    df_eng = pd.read_csv(eng_path)
    upload_csv_to_bigquery(df_eng, f"{project}.{dataset}.fact_engagement")

    perf_folder = get_latest_staging_folder("data/staging/performance")
    perf_path = os.path.join(perf_folder, "performance_cleaned.csv")
    df_perf = pd.read_csv(perf_path)
    upload_csv_to_bigquery(df_perf, f"{project}.{dataset}.fact_performance")

    master_folder = get_latest_staging_folder("data/staging/master")
    master_path = os.path.join(master_folder, "master_dataset.csv")
    df_master = pd.read_csv(master_path)
    upload_csv_to_bigquery(df_master, f"{project}.{dataset}.master_dataset")

    print("\nAll tables uploaded to BigQuery successfully!")

if __name__ == "__main__":
    run_load_bigquery()