import os
import pandas as pd

def get_latest_staging_folder(base_path: str) -> str:
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Staging path does not exist: {base_path}")

    folders = [
        f for f in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, f))
    ]

    if not folders:
        raise FileNotFoundError(f"No staging folders found in {base_path}")

    folders.sort(reverse=True)
    return os.path.join(base_path, folders[0])

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def run_master_transform():
    print("\nRunning Master Transform\n")

    emp_folder = get_latest_staging_folder("data/staging/employee")
    att_folder = get_latest_staging_folder("data/staging/attendance")
    eng_folder = get_latest_staging_folder("data/staging/engagement")
    perf_folder = get_latest_staging_folder("data/staging/performance")

    df_emp = pd.read_csv(os.path.join(emp_folder, "employee_cleaned.csv"))
    df_att = pd.read_csv(os.path.join(att_folder, "attendance_cleaned.csv"))
    df_eng = pd.read_csv(os.path.join(eng_folder, "engagement_cleaned.csv"))
    df_perf = pd.read_csv(os.path.join(perf_folder, "performance_cleaned.csv"))

    if "status" in df_att.columns:
        df_att["presence_flag"] = (df_att["status"] == "Present").astype(int)
    else:
        raise ValueError("attendance_cleaned.csv must contain 'status' column")

    attendance_summary = (
        df_att
        .groupby("employee_id", as_index=False)
        .agg(
            presence_score=("presence_flag", "sum"),
            total_hours=("hours_worked", "sum"),
            late_count=("is_late", "sum"),
            overtime_count=("is_overtime", "sum"),
        )
    )

    eng_numeric_cols = (
        df_eng
        .select_dtypes(include="number")
        .columns
        .drop("employee_id", errors="ignore")
    )

    if len(eng_numeric_cols) == 0:
        raise ValueError("No numeric engagement columns found to aggregate")

    engagement_summary = (
        df_eng
        .groupby("employee_id")[eng_numeric_cols]
        .mean()
        .reset_index()
        .rename(columns=lambda c: f"eng_{c}" if c != "employee_id" else c)
    )

    if "review_date" not in df_perf.columns:
        raise ValueError("performance_cleaned.csv must contain 'review_date' column")

    performance_latest = (
        df_perf
        .sort_values("review_date")
        .groupby("employee_id", as_index=False)
        .last()
        .rename(columns=lambda c: f"perf_{c}" if c != "employee_id" else c)
    )

    master = df_emp.copy()

    master = master.merge(attendance_summary, on="employee_id", how="left")
    master = master.merge(engagement_summary, on="employee_id", how="left")
    master = master.merge(performance_latest, on="employee_id", how="left")

    numeric_cols = master.select_dtypes(include="number").columns
    master[numeric_cols] = master[numeric_cols].fillna(0)

    extract_date = os.path.basename(emp_folder)
    output_folder = f"data/staging/master/{extract_date}"
    ensure_dir(output_folder)

    output_path = os.path.join(output_folder, "master_dataset.csv")
    master.to_csv(output_path, index=False)

    print(f"Master dataset saved to: {output_path}")
    print("\nMaster Transformation Complete\n")


if __name__ == "__main__":
    run_master_transform()
