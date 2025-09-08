# First creating the file
import csv
import random

import pandas as pd

headers = [["order_id","customer_id","order_date","product","quantity","price"]]

with open("sales_data.csv", "w+") as file:
    writer = csv.writer(file)

    writer.writerows(headers)

    for i in range(1,51):
        data = [[f"order_{random.choice(range(1,51))}",
                f"customer_{random.choice(range(1,51))}",
                "date",
                f"product_{random.choice(range(1,51))}",
                f"{random.choice(range(1,1001))}",
                f"{random.choice(range(1,2001))}"]]

        # print(data)

        writer.writerows(data)


df = pd.read_csv("sales_data.csv")
print(df)

print(df.info())
