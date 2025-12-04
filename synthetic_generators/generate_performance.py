import pandas as pd
import numpy as np
import os

def generate_performance(employee_ids):
    review_cycles = [
        ("2021-12-15", "Annual"),
        ("2022-06-15", "Mid-Year"),
        ("2022-12-15", "Annual"),
        ("2023-06-15", "Mid-Year"),
    ]

    performance_rows = []

    for emp in employee_ids:
        for date, cycle in review_cycles:

            # overall rating distribution
            overall_rating = np.random.choice(
                [1, 2, 3, 4, 5],
                p=[0.05, 0.15, 0.55, 0.20, 0.05]
            )

            # goals score ~ correlated with rating
            goals_score = round(np.random.normal(overall_rating * 0.8, 0.4), 2)
            goals_score = min(max(goals_score, 1), 5)

            # manager score ~ slightly noisy
            manager_score = round(np.random.normal(overall_rating * 0.85, 0.5), 2)
            manager_score = min(max(manager_score, 1), 5)

            # potential rating (low/medium/high)
            potential = np.random.choice(
                ["Low", "Medium", "High"],
                p=[0.2, 0.6, 0.2]
            )

            # bonus % based on performance
            bonus_pct = {
                1: 0,
                2: 3,
                3: 6,
                4: 10,
                5: 15
            }[overall_rating]

            # promotion recommendation
            promotion = np.random.choice(
                ["Yes", "No"],
                p=[0.12, 0.88]
            )

            performance_rows.append([
                emp,
                date,
                cycle,
                overall_rating,
                goals_score,
                manager_score,
                potential,
                bonus_pct,
                promotion
            ])

    df = pd.DataFrame(performance_rows, columns=[
        "employee_id",
        "review_date",
        "review_cycle",
        "overall_rating",
        "goals_score",
        "manager_score",
        "potential_rating",
        "bonus_percentage",
        "promotion_recommendation"
    ])

    return df


if __name__ == "__main__":
    raw_path = "data/raw/ibm_hr/WA_Fn-UseC_-HR-Employee-Attrition.csv"
    df_emp = pd.read_csv(raw_path)
    employee_ids = df_emp["EmployeeNumber"].unique()

    df_perf = generate_performance(employee_ids)

    output_folder = "data/raw/performance/"
    os.makedirs(output_folder, exist_ok=True)

    output_path = os.path.join(output_folder, "performance_reviews.csv")
    df_perf.to_csv(output_path, index=False)

    print(f"Performance dataset created: {output_path}")
