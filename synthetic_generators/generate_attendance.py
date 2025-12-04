import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_attendance(employee_ids, start_date="2022-01-01", end_date="2022-12-31"):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    dates = pd.date_range(start, end, freq="B")  # Business days

    logs = []

    for emp in employee_ids:
        for d in dates:
            status = np.random.choice(
                ["Present", "Absent", "Remote", "Sick Leave"],
                p=[0.94, 0.03, 0.02, 0.01]
            )

            if status == "Present":
                hours = np.random.uniform(7, 10)
            elif status == "Remote":
                hours = np.random.uniform(6, 9)
            else:
                hours = 0.0

            if status in ["Present", "Remote"]:
                is_late = np.random.choice([0, 1], p=[0.9, 0.1])
            else:
                is_late = 0

            is_overtime = 1 if hours > 8.5 else 0

            logs.append([
                int(emp),
                d.date(),
                status,
                round(hours, 2),
                is_late,
                is_overtime
            ])

    df = pd.DataFrame(logs, columns=[
        "employee_id",
        "date",
        "status",
        "hours_worked",
        "is_late",
        "is_overtime"
    ])

    return df


if __name__ == "__main__":
    raw_path = "data/raw/ibm_hr/WA_Fn-UseC_-HR-Employee-Attrition.csv"
    df_emp = pd.read_csv(raw_path)

    employee_ids = df_emp["EmployeeNumber"].unique()

    attendance_df = generate_attendance(employee_ids)

    output_folder = "data/raw/attendance/"
    os.makedirs(output_folder, exist_ok=True)

    output_path = os.path.join(output_folder, "attendance_logs.csv")
    attendance_df.to_csv(output_path, index=False)

    print(f"Attendance dataset created: {output_path}")
