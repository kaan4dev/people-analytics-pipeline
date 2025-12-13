import os
import duckdb
import pandas as pd

def get_latest_staging_folder(path:str) -> str:
    folders = [
        f for f in os.listdir(path)
        if os.path.isdir(os.path.join(path, f))
    ]

    if not folders:
        raise FileNotFoundError(f"No staging folder exists in: {path}")
    
    folders.sort(reverse=True)
    return os.path.join(path, folders[0])

def load_dimensions():
    print("Loading dimension tables...")
    
    con = duckdb.connect("data/warehouse/people_analytics.duckdb")

    emp_folder = get_latest_staging_folder("data/staging/employee")
    emp_path = os.path.join(emp_folder, "employee_cleaned.csv")

    df_emp = pd.read_csv(emp_path)
    con.execute("CREATE OR REPLACE TABLE dim_employee AS SELECT * FROM df_emp")

    master_folder = get_latest_staging_folder("data/staging/master")
    master_path = os.path.join(master_folder, "master_dataset.csv")

    df_master = pd.read_csv(master_path)
    con.execute("CREATE OR REPLACE master_dataset AS SELECT * FROM df_master")
    print(f"Loaded master_dataset from: {master_path}")

    con.close()
    print("Dimension load completed.")
