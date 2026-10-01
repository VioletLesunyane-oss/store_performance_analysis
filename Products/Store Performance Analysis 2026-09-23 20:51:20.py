# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Data Ingestion and Conversion: Products DataFrame

# COMMAND ----------

# 1. Loading and Displaying the Products Data
# This code loads the Products table from Databricks, converts it from a Spark DataFrame into a Pandas DataFrame, and then displays the data for inspection and analysis.

# This reads the products table from the store_performance_analysis default.products using Spark and stores it in a variable called products.
products=spark.read.table("store_performance_analysis.default.products")

# This converts the Spark DataFrame into a Pandas DataFrame, making it easier to perform Python and Pandas-based analysis.
products = products.toPandas()

# This displays the Products DataFrame as a table so i can view the records and check the data before continuing with the analysis.
display(products)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion and Conversion: Orders DataFrame

# COMMAND ----------

# 2. Loading and Displaying the Orders Data
# This code loads the Orders table, converts it from a Spark DataFrame into a Pandas DataFrame, and displays the data for inspection and analysis.

# This reads the products table from the store_performance_analysis.default.orders using Spark and stores it in a variable called Orders.
orders=spark.read.table("store_performance_analysis.default.orders")

# This Converts the Spark DataFrame into a Pandas DataFrame, making it easier to perform Python and Pandas-based analysis.
orders = orders.toPandas()

# Displays the Orders data as a table, allowing further data inspection.
display(orders)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion and Conversion: Payments DataFrame

# COMMAND ----------

# 3. Loading and Displaying the Payments Data
# This code loads the Payments table, converts it from a Spark DataFrame into a Pandas DataFrame, and displays the data for inspection and analysis.

# This reads the payments table from the store_performance_analysis.default.payments using Spark and stores it in a variable called Payments.
payments=spark.read.table("store_performance_analysis.default.payments")

# This converts the Spark DataFrame into a Pandas DataFrame, making it easier to perform Python and Pandas-based analysis.
payments = payments.toPandas()

# This displays the Payments data as a table, allowing further data inspection. 
display(payments)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion and Conversion: Customers DataFrame

# COMMAND ----------

# 4. Loading and Displaying the Customers Data
# This code loads the Customerss table, converts it from a Spark DataFrame into a Pandas DataFrame, and displays the data for inspection and analysis.

# This reads the payments table from the store_performance_analysis.default.customers using Spark and stores it in a variable called Customenrs.
customers=spark.read.table("store_performance_analysis.default.customers")
 # This converts the Spark DataFrame into a Pandas DataFrame, making it easier to perform Python and Pandas-based analysis.
customers = customers.toPandas()

# This displays the Customers data as a table, allowing further data inspection.
display(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing Pandas and Numpy Libraries

# COMMAND ----------

# These two lines import Python libraries that are commonly used for data cleaning, calculations, aggregations and analysis.
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Products DataFrame EDA

# COMMAND ----------

# 1.1. Checking the Size of the Products Dataset
# This code displays the number of rows and columns in the products DataFrame.
display(products.shape)

# COMMAND ----------

# 1.2. Checking the Structure of the Products DataFtrame.
# This code provides a summary of the structure of the products DataFrame, including its columns, number of non-null values, and data types.
products.info()

# COMMAND ----------

# 1.3. Generating Descriptive Statistics
# This code generates descriptive statistics for the numerical columns in the products DataFrame, including the count, average, standard deviation, minimum, quartiles and maximum values. The .round(2) rounds the results to two decimal places, making the statistics easier to read.
products.describe().round(2)

# COMMAND ----------

# 1.4. This code displays first 10 rows of the DataFrame.
display(products.head(10))

# COMMAND ----------

# 1.5. This code displays last 10 rows of the DataFrame.
display(products.tail(10))

# COMMAND ----------

# 1.6. Checking for Nulls and Empty Values
# This code checks each column in the products DataFrame for null or missing values and counts how many null values are present in each column. It is useful for identifying columns that have missing data before cleaning or analysing the dataset.
products.isna().sum()

# COMMAND ----------

products.duplicated().sum()

# COMMAND ----------

products["ProductID"].count()

# COMMAND ----------

products["ProductID"].value_counts()

# COMMAND ----------

products["Category"].nunique()

# COMMAND ----------

products["Category"].value_counts()

# COMMAND ----------

display(products.loc[
    products.groupby("Category")["UnitPrice"].agg(["idxmax", "idxmin"]).stack()
].sort_values(["Category", "UnitPrice"]))

# COMMAND ----------

products["UnitPrice"].sum()

# COMMAND ----------

display(
    products.groupby("Category").agg(
        Category_Count=("ProductName", "count"),
        Total_Unit_Price=("UnitPrice", "sum")
    ).sort_values("Total_Unit_Price", ascending=False).reset_index()
)

# COMMAND ----------

display(products.groupby("Category")["UnitPrice"].agg(
    Mean="mean",
    Minimum="min",
    Maximum="max"
).round(2).reset_index())

# COMMAND ----------

# Avrage Unit Price = Total Number Of UnitPrice/Total Number of Products
products["UnitPrice"].agg(
    Mean="mean",
    Minimum="min",
    Maximum="max",
    Count="count"
).sort_values(ascending=False).round(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Orders DataFrame EDA

# COMMAND ----------

display(orders)

# COMMAND ----------

orders.info()

# COMMAND ----------

# Converting DataTypes to correct DataTypes
orders["OrderDate"] = pd.to_datetime(orders["OrderDate"])
orders["Quantity"] = orders["Quantity"].astype("Int64")

# COMMAND ----------

display(orders.dtypes)

# COMMAND ----------

# Duration of the data
start_date = orders["OrderDate"].min()
end_date = orders["OrderDate"].max()

display("Start Date:", start_date)
display("End Date:", end_date)

# COMMAND ----------

display(orders.shape)

# COMMAND ----------

display(orders.head(10))

# COMMAND ----------

display(orders.tail(10))

# COMMAND ----------

orders.describe().round(2)

# COMMAND ----------

display(orders.isna().sum())

# COMMAND ----------

display(orders.duplicated().sum())

# COMMAND ----------

display(
    orders.apply(lambda col: col.duplicated().sum())
)

# COMMAND ----------

# Getting a clear view of all duplicates
display(
    orders[orders["OrderID"].duplicated(keep=False)]
    .sort_values("OrderID")
)

# COMMAND ----------

# Checking how many duplicates does each order id hav
display(
    orders["OrderID"]
    .value_counts()
    .loc[lambda x: x > 1]
    .reset_index(name="Duplicate Count")
    .rename(columns={"OrderID": "OrderID"})
)

# COMMAND ----------

orders = orders.drop_duplicates(
    subset=["OrderID"],
    keep="first"
)

# COMMAND ----------

# Checking if duplicates were removed successfully under the orderid column
display(
    orders.apply(lambda col: col.duplicated().sum())
)

# COMMAND ----------

# Showing how many times one customer id appears 
display(
    orders["CustomerID"]
    .value_counts()
    .loc[lambda x: x > 1]
    .reset_index(name="Duplicate Count")
    .rename(columns={"CustomerID": "CustomerID"})
)

# COMMAND ----------


orders["OrderDate"].mode()

# COMMAND ----------

orders["OrderDate"] = orders["OrderDate"].fillna(orders["OrderDate"].mode()[0])

# COMMAND ----------

# Extracting dates using Orderdate
orders["Year"] = orders["OrderDate"].dt.year
orders["MonthName"] = orders["OrderDate"].dt.month_name()
orders["Day"] = orders["OrderDate"].dt.day
orders["DayName"] = orders["OrderDate"].dt.day_name()
orders["Quarter"] = orders["OrderDate"].dt.quarter

display(orders)

# COMMAND ----------

# Clasifying day of week using the OrderDate column
orders["day_classification"] = np.where(
    orders["OrderDate"].dt.dayofweek < 5,
    "Weekday",
    "Weekend"
)

display(orders)

# COMMAND ----------

orders["Season"] = orders["OrderDate"].dt.month.map({
    12: "Summer", 1: "Summer", 2: "Summer",
    3: "Autumn", 4: "Autumn", 5: "Autumn",
    6: "Winter", 7: "Winter", 8: "Winter",
    9: "Spring", 10: "Spring", 11: "Spring"
})

display(orders)

# COMMAND ----------

# MAGIC %md
# MAGIC - ProductID Column

# COMMAND ----------

orders["ProductID"].value_counts()

# COMMAND ----------

# Checking if the products ProductID in products table contain same data as products id in orders table
orders["ProductID"].isin(products["ProductID"]).value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Quantity Column

# COMMAND ----------

orders["Quantity"].dtypes


# COMMAND ----------

orders["Quantity"].shape

# COMMAND ----------

orders["Quantity"].isnull().sum()
   # as a percentage

# COMMAND ----------

orders["Quantity"].isnull().mean() * 100

# COMMAND ----------

orders["Quantity"].sum()

# COMMAND ----------

orders["Quantity"].value_counts()

# COMMAND ----------

(orders["Quantity"].value_counts(normalize=True) * 100).round(2)

# COMMAND ----------

orders["Quantity"].describe().round(2)


# COMMAND ----------

orders["Quantity"].agg(["mean", "median", "std", "skew"]).round(2)


# COMMAND ----------

orders["Quantity"].mode()

# COMMAND ----------

orders["Quantity"].isna().sum()
orders["Quantity"].mean().round(2)
orders["Quantity"].median()


# COMMAND ----------

orders["Quantity"] = orders["Quantity"].fillna(orders.groupby("ProductID")["Quantity"].transform("median"))

# COMMAND ----------

orders.isna().sum()

# COMMAND ----------

# Quantities cant be a negative number
orders[orders["Quantity"] <= 0]

# COMMAND ----------

orders.loc[orders["Quantity"] <= 0, "Quantity"] = np.nan
orders["Quantity"] = orders["Quantity"].fillna(orders.groupby("ProductID")["Quantity"].transform("median"))

# COMMAND ----------

orders["Quantity"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Discount Column

# COMMAND ----------

orders["Discount"].dtype

# COMMAND ----------


orders["Discount"].isna().sum()


# COMMAND ----------

orders["Discount"].value_counts(dropna=False).sort_index()

# COMMAND ----------

orders["Discount"].describe().round(2)


# COMMAND ----------

orders["Discount"].mode()[0]

# COMMAND ----------

orders["Discount"].skew().round(2) 

# COMMAND ----------

# Discount distribution
orders["Discount"].value_counts(normalize=True).sort_index().round(2) * 100

# COMMAND ----------

orders.groupby("Quantity")["Discount"].median()

# COMMAND ----------

orders["Discount"] = orders["Discount"].fillna(orders["Discount"].median())

# COMMAND ----------

orders["Discount"].isna().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment Method Column

# COMMAND ----------

orders["PaymentMethod"].dtype

# COMMAND ----------

orders["PaymentMethod"].isna().sum()

# COMMAND ----------

orders["PaymentMethod"].value_counts()

# COMMAND ----------

orders["PaymentMethod"].value_counts(normalize=True).round(2) * 100

# COMMAND ----------

#Replacing Nulls and missing values withon the PaymentMethod column with Unknows because it is a categorical field.
orders["PaymentMethod"] = orders["PaymentMethod"].fillna("Unknown")

# COMMAND ----------

orders.isna().sum()

# COMMAND ----------

orders["PaymentMethod"].value_counts()


# COMMAND ----------

# MAGIC %md
# MAGIC - Status Column

# COMMAND ----------

orders["Status"].isna().sum()

# COMMAND ----------

orders["Status"].value_counts()

# COMMAND ----------

orders["Status"].value_counts(normalize=True).round(2) * 100

# COMMAND ----------

pd.crosstab(orders["ProductID"], orders["Status"], normalize="index").round(2)

# COMMAND ----------

orders["ProductID"].isin(products["ProductID"]).value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ##JOINING ORDERS TABLE WITH PRODUCTS TABLE

# COMMAND ----------

orders_products = orders.merge(
    products,
    on="ProductID",
    how="left"
)

# COMMAND ----------

display(orders_products)

# COMMAND ----------

display(orders_products.shape)

# COMMAND ----------

orders_products.info()

# COMMAND ----------

orders_products.isna().sum()

# COMMAND ----------

orders_products.duplicated().sum()

# COMMAND ----------

orders_products.describe().round(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##CUSTOMERS TABLE EDA