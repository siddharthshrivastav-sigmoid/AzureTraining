# First creating the file
import csv
import random
from datetime import datetime, timedelta
import pandas as pd

headers = [["order_id","customer_id","order_date","product","quantity","price"]]

start = datetime(2022, 1, 1).date()
end = datetime(2022, 12, 31).date()

with open("sales_data.csv", "w+") as file:
    writer = csv.writer(file)

    writer.writerows(headers)

    for i in range(1,51):
        delta = end - start
        random_days = random.randint(0, delta.days)
        data = [[f"order_{random.choice(range(1,51))}",
                f"customer_{random.choice(range(1,51))}",
                f"{start + timedelta(days=random_days)}",
                f"product_{random.choice(range(1,51))}",
                f"{random.choice(range(1,1001))}",
                f"{random.choice(range(1,2001))}"]]

        # print(data)

        writer.writerows(data)


df = pd.read_csv("sales_data.csv")

df = df.dropna()
df = df.drop_duplicates()

print(df.describe().T)

# Finding revenue for all products
df["revenue"] = df["quantity"] * df["price"]
revenue_per_product = df.groupby("product")["revenue"].sum()
revenue_per_product.to_csv("product_revenue.csv")

# Top 5 customers by spending
top_customers = df.groupby("customer_id")["revenue"].sum()
ordered_customers = top_customers.sort_values(ascending = False).head(5)
ordered_customers.to_csv("top_customers.csv")


# Count how many orders where places each month
df["order_date"] = pd.to_datetime(df["order_date"])
df["month"] = df["order_date"].dt.month
orders_perMonth = df.groupby("month")["revenue"].sum()
orders_perMonth.to_csv("monthly_order.csv")
