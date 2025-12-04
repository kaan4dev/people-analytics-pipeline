import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

N_EMPLOYEES = 1470  
SURVEYS_PER_EMPLOYEE = 3
START_DATE = datetime(2021, 1, 1)
END_DATE = datetime(2023, 12, 31)

def likert():
    return np.random.randint(1, 6)

def generate_engagement_surveys():
    rows = []
    survey_id = 10000

    for emp_id in range(1, N_EMPLOYEES + 1):

        n_surveys = np.random.poisson(SURVEYS_PER_EMPLOYEE)
        if n_surveys < 1:
            n_surveys = 1

        for _ in range(n_surveys):
            survey_id += 1
            survey_date = fake.date_between_dates(START_DATE, END_DATE)

            row = {
                "survey_id": survey_id,
                "employee_id": emp_id,
                "survey_date": survey_date,
                "q_work_life_balance": likert(),
                "q_manager_support": likert(),
                "q_growth_opportunity": likert(),
                "q_recognition": likert(),
                "comment_text": fake.sentence(nb_words=12)
            }

            rows.append(row)

    df = pd.DataFrame(rows)
    return df

if __name__ == "__main__":
    df = generate_engagement_surveys()
    df.to_csv("engagement_surveys.csv", index=False)
    print("Generated engagement_surveys.csv with", len(df), "rows")
