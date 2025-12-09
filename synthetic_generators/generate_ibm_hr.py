import os
import random
import pandas as pd
from datetime import datetime, timedelta

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def random_date(start, end):
    """Return a random datetime between start and end (inclusive)."""
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def generate_ibm_hr_dataset(n=1470):
    departments = [
        "Sales", "Human Resources", "R&D", "Engineering", "Finance",
        "Marketing", "Customer Success", "IT", "Operations"
    ]

    job_roles = {
        "Sales": ["Sales Representative", "Account Manager", "Sales Executive"],
        "Human Resources": ["HR Specialist", "Recruiter", "HR Manager"],
        "R&D": ["Research Scientist", "Lab Technician", "Principal Researcher"],
        "Engineering": ["Software Engineer", "DevOps Engineer", "Data Engineer"],
        "Finance": ["Financial Analyst", "Accountant", "Finance Manager"],
        "Marketing": ["Marketing Analyst", "SEO Specialist", "Brand Manager"],
        "Customer Success": ["Support Associate", "Customer Success Manager"],
        "IT": ["Systems Admin", "IT Support", "Network Engineer"],
        "Operations": ["Operations Analyst", "Logistics Coordinator"]
    }

    education_levels = ["High School", "Associate's", "Bachelor's", "Master's", "PhD"]
    genders = ["Male", "Female", "Nonbinary"]
    marital_statuses = ["Single", "Married", "Divorced"]
    employment_statuses = ["Active", "Terminated", "Leave of Absence"]

    data = []

    start_date = datetime(2005, 1, 1)
    end_date = datetime(2022, 12, 31)

    for i in range(1, n + 1):
        dept = random.choice(departments)
        role = random.choice(job_roles[dept])

        hire_date = random_date(start_date, end_date)

        monthly_income = random.randint(3000, 20000)

        employee = {
            "EmployeeID": i,
            "FirstName": f"Emp{i}",
            "LastName": f"LN{i}",
            "Age": random.randint(20, 60),
            "Gender": random.choice(genders),
            "MaritalStatus": random.choice(marital_statuses),
            "Department": dept,
            "JobRole": role,
            "MonthlyIncome": monthly_income,
            "EducationLevel": random.choice(education_levels),
            "HireDate": hire_date.strftime("%Y-%m-%d"),
            "EmploymentStatus": random.choice(employment_statuses),
            "ManagerID": random.randint(1, n//10) if i > 10 else None,
            "BusinessTravel": random.choice(["Travel_Rarely", "Travel_Frequently", "Non-Travel"]),
            "OverTime": random.choice(["Yes", "No"])
        }

        data.append(employee)

    df = pd.DataFrame(data)
    return df


def run():
    print("Generating synthetic IBM HR Core Employee dataset...")

    target_dir = "data/raw/ibm_hr"
    ensure_dir(target_dir)

    df = generate_ibm_hr_dataset()

    output_path = os.path.join(target_dir, "employee_data.csv")
    df.to_csv(output_path, index=False)

    print(f"Employee HR dataset created: {output_path}")
    print(f"Rows generated: {len(df)}")


if __name__ == "__main__":
    run()
