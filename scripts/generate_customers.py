
import csv
import math
import random
from pathlib import Path

RNG = random.Random(42)
RECORD_COUNT = 1000
OUTPUT = Path("data/customers_synthetic.csv")


def generate_customer(index):
    gender = RNG.choice(["Male", "Female"])
    age = RNG.randint(18, 75)
    tenure = RNG.randint(1, 72)
    monthly_charges = round(RNG.uniform(25, 150), 2)

    total_charges = round(
        monthly_charges * tenure * RNG.uniform(0.95, 1.05), 2
    )

    # Synthetic churn probability: newer customers and
    # customers with higher charges are more likely to churn.
    score = (
        -0.7
        - 0.045 * tenure
        + 0.018 * (monthly_charges - 70)
        + 0.012 * (age - 40)
    )
    probability = 1 / (1 + math.exp(-score))
    churn = "Yes" if RNG.random() < probability else "No"

    return [
        f"SYN{index:05d}",
        gender,
        age,
        tenure,
        monthly_charges,
        total_charges,
        churn,
    ]


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "customer_id",
        "gender",
        "age",
        "tenure_months",
        "monthly_charges",
        "total_charges",
        "churn",
    ]

    records = [
        generate_customer(i)
        for i in range(1, RECORD_COUNT + 1)
    ]

    with OUTPUT.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(records)

    yes_count = sum(row[-1] == "Yes" for row in records)

    print(f"Generated {len(records)} customers")
    print(f"Churn Yes: {yes_count}")
    print(f"Churn No: {len(records) - yes_count}")
    print(f"Output: {OUTPUT.resolve()}")


if __name__ == "__main__":
    main()