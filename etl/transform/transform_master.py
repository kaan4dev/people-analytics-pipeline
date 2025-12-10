import os 
import pandas as pd

def get_latest_staging_folder(base_path: str) -> str:
    folders = [
        f for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f))
    ]
    if not folders:
        raise FileNotFoundError(f"No staging folders found in {base_path}")

    folders.sort(reverse=True)
    return os.path.join(base_path, folders[0])

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def run_master_transform():
    emp_folder = get_latest_staging_folder("data/staging/employee")
    att_folder = get_latest_staging_folder("data/staging/attendance")
    eng_folder = get_latest_staging_folder("data/staging/engagement")
    perf_folder = get_latest_staging_folder("data/staging/performance")

    df_emp = pd.read_csv(os.path.join(emp_folder, "employee_cleaned.csv"))
    df_att = pd.read_csv(os.path.join(att_folder, "attendance_cleaned.csv"))
    df_eng = pd.read_csv(os.path.join(eng_folder, "engagement_cleaned.csv"))
    df_perf = pd.read_csv(os.path.join(perf_folder, "performance_cleaned.csv"))

    attendance_summary = (
        df_att.groupby("employee_id")
              .agg({
                   "status": "sum",       
                   "hours_worked": "sum",
                   "is_late": "sum",
                   "is_overtime": "sum"
              })
              .reset_index()
              .rename(columns={
                   "status": "presence_score",
                   "hours_worked": "total_hours",
                   "is_late": "late_count",
                   "is_overtime": "overtime_count"
              })
    )

    engagement_summary = (
        df_eng.groupby("employee_id")
        .agg({
            c: "mean" for c in df_eng.columns if c not in ["employee_id", "survey_date", "year", "month"]
        })
        .reset_index()
        .rename(columns=lambda c: f"eng{c}" if c != "employee_id" else c)
    )
    
    df_perf_sorted = df_perf.sort_values(["employee_id", "review_date"])
    performance_latest = (
        df_perf_sorted.groupby("employee_id").tail(1).reset_index(drop=True)
    )
    performance_latest = performance_latest.rename(columns=lambda c: f"perf_{c}" if c not in ["employee_id"] else c)

    master = df_emp.copy()

    master = master.merge(attendance_summary, on="employee_id", how="left")
    master = master.merge(engagement_summary, on="employee_id", how="left")
    master = master.merge(performance_latest, on="employee_id", how="left")

    master = master.fillna(0)

    extract_date = os.path.basename(emp_folder)
    output_folder = f"data/staging/master/{extract_date}"
    ensure_dir(output_folder)

    output_path = os.path.join(output_folder, "master_dataset.csv")
    master.to_csv(output_path, index=False)

    print(f"Master dataset saved to: {output_path}")
    print("\n===== Master Transformation Complete =====\n")


if __name__ == "__main__":
    run_master_transform()