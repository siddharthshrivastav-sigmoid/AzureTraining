from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path("/Users/siddharthshrivastav/IdeaProjects/AzureTraining/Snowflake/snowpark/secrets.env")
load_dotenv(dotenv_path=env_path)

snowflake_pass = os.environ.get("SNOWFLAKE_PASSWORD")

connection_parameters = {
    "account": "wrxbrkh-my02936",
    "user": "siddharth912",
    "password": snowflake_pass,
    "role": "ACCOUNTADMIN",
    "warehouse": "LAB_WH",
    "database": "SNOWPARK_DB",
    "schema": "PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

session.sql("SELECT CURRENT_DATE").show()

# Dataframe API
# Load a table into a DataFrame:
df = session.table("CUSTOMERS")
# Select specific columns:
df.select("NAME", "REGION").show()
# Filter rows:
df.filter(col("REGION") == "APAC").show()
# Perform aggregation:
df.group_by("REGION").count().show()
# Join with another table:
orders = session.table("ORDERS")
joined = df.join(orders, df["ID"] == orders["CUSTOMER_ID"])
joined.show()

# UDF lab
# Define a function:
def add_discount(price: float) -> float:
    return price * 0.9

# Register UDF:
from snowflake.snowpark.functions import udf, FloatType
discount_udf = udf(add_discount, return_type = FloatType())

# Apply to DataFrame:

df = session.table("ORDERS")
df = df.with_column("discounted_price", discount_udf(col("AMOUNT")))
df.show()


# ML Lab
# Prepare features:
# df = session.table("CUSTOMERS")
# features = df.select("AGE", "INCOME", "PURCHASE_AMOUNT").to_pandas()

# Train ML model in Python:
# from sklearn.linear_model import LinearRegression
# model = LinearRegression()
# model.fit(features[["AGE", "INCOME"]], features["PURCHASE_AMOUNT"])
# # Deploy as UDF (mock example):
# def predict_purchase(age: int, income: float) -> float:
#     return model.predict([[age, income]])[0]
#
# predict_udf = udf(predict_purchase, return_type=FloatType())
# # Use in queries:
# df = df.with_column("predicted_purchase", predict_udf(col("AGE"), col("INCOME")))
# df.show()

# These columns dont even exist
