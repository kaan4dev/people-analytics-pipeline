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

def load_facts():
    print("Loading fact tables...")

    con = duckdb.connect("data/warehouse/people_analytics.duckdb")

    att_folder = get_latest_staging_folder("data/staging/attendance")
    att_path = os.path.join(att_folder, "attendance_cleaned.csv")

    df_att = pd.read_csv(att_path)
    con.execute("CREATE OR REPLACE TABLE fact_attendance AS SELECT * FROM df_att")
    print(f"Loaded fact_attendance from: {att_path}")

    eng_folder = get_latest_staging_folder("data/staging/engagement")
    enp_path = os.path.join(eng_folder, "engagement_cleaned.csv")

    df_eng = pd.read_csv(eng_path)
    con.execute("CREATE OR REPLACE TABLE fact_engagement AS SELECT * FROM df_eng")
    print(f"Loaded fact_engagement from: {eng_path}")

    perf_folder = get_latest_staging_folder("data/staging/performance")
    perf_path = os.path.join(perf_folder, "performance_cleaned.csv")

    df_perf = pd.read_csv(perf_path)
    con.execute("CREATE OR REPLACE TABLE fact_performance AS SELECT * FROM df_perf")
    print(f"Loaded fact_performance from: {perf_path}")

    con.close()
    print("Facts load complete.\n")