# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC

# COMMAND ----------

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

# MAGIC %md
# MAGIC - ProductID Column

# COMMAND ----------

products["ProductID"].count()

# COMMAND ----------

products["ProductID"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Category Column

# COMMAND ----------

products["Category"].nunique()

# COMMAND ----------

products["Category"].value_counts()

# COMMAND ----------

display(products.loc[
    products.groupby("Category")["UnitPrice"].agg(["idxmax", "idxmin"]).stack()
].sort_values(["Category", "UnitPrice"]))

# COMMAND ----------

# MAGIC %md
# MAGIC - Unit Price

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

# COMMAND ----------

display(customers)

# COMMAND ----------

display(customers.shape)

# COMMAND ----------

#Age column must be converted to an integer and signupdate to datetime
customers.info()

# COMMAND ----------

customers.isna().sum()

# COMMAND ----------

customers.duplicated().sum()

# COMMAND ----------

customers.describe().round(2)

# COMMAND ----------

# MAGIC %md
# MAGIC - CustomerID Column

# COMMAND ----------

customers["CustomerID"].count()

# COMMAND ----------


customers["CustomerID"].isna().sum()

# COMMAND ----------

customers["CustomerID"].duplicated().sum()

# COMMAND ----------

# Comparing customerID column in orders_products table with customers ID. We do not have duplicates in this column
orders_products["CustomerID"].isin(customers["CustomerID"]).value_counts()

# COMMAND ----------

customers["CustomerID"].is_unique

# COMMAND ----------

# CustomerID column is a clean primary key.
# 100001 to 110000 is exactly 10,000 numbers, matching the row count.
customers["CustomerID"].agg(["min", "max"])

# COMMAND ----------

customers["CustomerID"].diff().dropna().eq(1).all()

# COMMAND ----------

# MAGIC %md
# MAGIC - Age Column

# COMMAND ----------

customers.info()

# COMMAND ----------

customers["Age"] = customers["Age"].astype("Int64")

# COMMAND ----------

customers["Age"].info()

# COMMAND ----------

customers["Age"].count()

# COMMAND ----------

display(customers["Age"].value_counts())

# COMMAND ----------

customers["Age"].isna().sum()

# COMMAND ----------

customers["Age"].describe().round(0)

# COMMAND ----------

customers["Age"].mode()

# COMMAND ----------

#Comparing ages with percentages
customers["Age"].quantile([.05, .25, .5, .75, .95])

# COMMAND ----------

pd.cut(customers["Age"], [17, 24, 34, 44, 54, 65]).value_counts()

# COMMAND ----------

# In this instance using mean or median would have produced same results as the average age constituted 41 and median 41
customers["Age"] = customers["Age"].fillna(customers["Age"].median())

# COMMAND ----------

#Handled the nulls by replaing them with median age which is 41
customers["Age"].isna().sum()

# COMMAND ----------

display(customers["Age"].value_counts())

# COMMAND ----------

def age_bucket(age):
    if age >= 18 and age <= 24:
        return "Youth"
    elif age >= 25 and age <= 34:
        return "Young Adults"
    elif age >= 35 and age <= 54:
        return "Adults"
    else:
        return "Seniors"

customers["AgeBucket"] = customers["Age"].apply(age_bucket)

display(customers)

# COMMAND ----------

display(
    customers["AgeBucket"].value_counts().reset_index()
)

# COMMAND ----------

display(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC - City Column

# COMMAND ----------

customers["City"].isna().sum()

# COMMAND ----------

customers["City"] = customers["City"].fillna("Unknown")

# COMMAND ----------

customers["City"].isna().sum()

# COMMAND ----------

customers["City"].nunique()

# COMMAND ----------

customers["City"].value_counts()

# COMMAND ----------

customers["City"] = (customers["City"]
    .str.strip()
    .str.title()
    .replace({"Mashad": "Mashhad"}))


# COMMAND ----------

customers["City"].value_counts()

# COMMAND ----------

customers["City"].nunique()

# COMMAND ----------

# MAGIC %md
# MAGIC - Signup Date Column

# COMMAND ----------

customers["SignupDate"].info()

# COMMAND ----------

customers["SignupDate"] = pd.to_datetime(customers["SignupDate"])

display(customers)

# COMMAND ----------

start_date = customers["SignupDate"].min()
end_date = customers["SignupDate"].max()

display("Start Date:", start_date)
display("End Date:", end_date)

# COMMAND ----------

customers["Year"] = customers["SignupDate"].dt.year
customers["MonthName"] = customers["SignupDate"].dt.month_name()
customers["Day"] = customers["SignupDate"].dt.day
customers["DayName"] = customers["SignupDate"].dt.day_name()
customers["Quarter"] = customers["SignupDate"].dt.quarter

display(customers)

# COMMAND ----------

def classify_day(date):
    if date.dayofweek < 5:
        return "Weekday"
    else:
        return "Weekend"

customers["day_classification"] = customers["SignupDate"].apply(classify_day)

display(customers)

# COMMAND ----------

customers["SignupDate"].dt.to_period("Q").value_counts().sort_index()

# COMMAND ----------

customers["SignupDate"].dt.day_name().value_counts()

# COMMAND ----------

customers["day_classification"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Customer Segment

# COMMAND ----------

customers["CustomerSegment"].isna().sum()

# COMMAND ----------

customers["CustomerSegment"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOINING TABLES

# COMMAND ----------

ord_prod_cust = orders_products.merge(
    customers,
    on="CustomerID",
    how="left"
)

display(ord_prod_cust)

# COMMAND ----------

ord_prod_cust.shape

# COMMAND ----------

ord_prod_cust.info()

# COMMAND ----------

ord_prod_cust.isna().sum()

# COMMAND ----------

ord_prod_cust["IsGuest"] = ord_prod_cust["CustomerID"] == 999999

ord_prod_cust["IsGuest"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC In the below code,the data shows that we have 30 customers with missing data and i have decided to keep those customers for the following reasons:
# MAGIC
# MAGIC - Out of 50,000 orders, 30 (just 0.06%) were placed by guests, meaning customers who did not have an account, so we have no details for them such as age, city or signup date. 
# MAGIC - I chose to keep these orders in the data instead of deleting them because the sales themselves are real: 
# MAGIC - they add up to about 1,991 in sales out of a total of 3,503,500 (0.06%) and 49 items out of 94,172 sold (0.05%), and 28 of the 30 were completed. 
# MAGIC - Removing them would make our totals slightly smaller than the true business figures, and the numbers would no longer match the company's records. 
# MAGIC - The customer details for these orders were left blank instead of being guessed, because inventing an age or a city for someone we know nothing about would make the data less accurate, and the text fields were labelled "Unknown" so they are easy to spot. 
# MAGIC - A "guest" marker was added to these 30 orders so they can be left out whenever we study customer behaviour, such as age or city, without losing them from sales totals.

# COMMAND ----------

#Creating flags for customers that are just guests
ord_prod_cust["IsGuest"] = ord_prod_cust["CustomerID"] == 999999

for col in ["City", "CustomerSegment", "AgeBucket"]:
    ord_prod_cust[col] = ord_prod_cust[col].fillna("Unknown")

display(ord_prod_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PAYMENTS TABLE EDA

# COMMAND ----------

payments.shape

# COMMAND ----------

payments.info()

# COMMAND ----------

payments.isna().sum()

# COMMAND ----------

payments["OrderID"].duplicated().sum()

# COMMAND ----------

payments["OrderID"].nunique()

# COMMAND ----------

ord_prod_cust["OrderID"].isin(payments["OrderID"]).value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment ID Column

# COMMAND ----------

payments["PaymentID"].isna().sum()

# COMMAND ----------

payments["PaymentID"].duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Order ID Column

# COMMAND ----------

payments["OrderID"].isna().sum()

# COMMAND ----------

payments["OrderID"].duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment Date Column

# COMMAND ----------

payments["PaymentDate"].info()

# COMMAND ----------

payments["PaymentDate"] = pd.to_datetime(payments["PaymentDate"])

# COMMAND ----------

start_date = payments["PaymentDate"].min()
end_date = payments["PaymentDate"].max()

display("Start Date:", start_date)
display("End Date:", end_date)

# COMMAND ----------

ord_prod_cust["OrderDate"].isin(payments["PaymentDate"]).value_counts()

# COMMAND ----------

payments["PaymentDate"].info()

# COMMAND ----------

payments["PaymentDate"].isna().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Temporary Table to check both OrderDate and PaymentDate if the dates are exactly the same

# COMMAND ----------


side_by_side = ord_prod_cust[["OrderID", "OrderDate"]].merge(
    payments[["OrderID", "PaymentDate", "PaymentStatus"]],
    on="OrderID", how="left", validate="1:1")

side_by_side["Result"] = np.where(
    side_by_side["OrderDate"].isnull() & side_by_side["PaymentDate"].isnull(), "Both missing",
    np.where(side_by_side["OrderDate"] == side_by_side["PaymentDate"], "Match", "Different"))

side_by_side.fillna("(missing)").head(10)        # show nulls as words instead of NaN
side_by_side["Result"].value_counts()

display(side_by_side)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOINING TABLES

# COMMAND ----------

store_table = ord_prod_cust.merge(payments[["OrderID", "PaymentStatus"]],
    on="OrderID", 
    how="left", validate="1:1")

display(store_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FINAL ANALYSIS

# COMMAND ----------

# MAGIC %md
# MAGIC - Data Exoploration

# COMMAND ----------

store_table.shape

# COMMAND ----------

store_table.info()

# COMMAND ----------

store_table["Year_y"]    = store_table["Year_y"].astype("Int64")
store_table["Day_y"]     = store_table["Day_y"].astype("Int64")
store_table["Quarter_y"] = store_table["Quarter_y"].astype("Int64")

display(store_table)

# COMMAND ----------

# MAGIC %md
# MAGIC - NOTE: The 30 nulls below represent guests that we flagged

# COMMAND ----------

store_table.isna().sum()

# COMMAND ----------

store_table.duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue and Net Revenue

# COMMAND ----------

# MAGIC %md
# MAGIC # Revenue
# MAGIC
# MAGIC - Is the total money the business earned from selling products befor any deductins (e.g. Discounts)
# MAGIC - Meaning its Total Sales before deductions.
# MAGIC - Formula is Price x Quantity Sold.
# MAGIC
# MAGIC # Net Revenue 
# MAGIC
# MAGIC - Is Revenue minust customers returns, discounts, etc.
# MAGIC - Formila is Revenue - Discounts
# MAGIC - However, in this instance, we will be using Quntity x Unit Price x (1 - Discount) to get Net Revenue for a specific sale or product line.
# MAGIC - Of-which, by multiplying the standard price by (1 - Discount), we will be getting an actual discounted unit price paid by the customer.

# COMMAND ----------

store_table["Revenue"] = (
    store_table["Quantity"]
    * store_table["UnitPrice"]
    * (1 - store_table["Discount"])
)

store_table["NetRevenue"] = np.where(
    (store_table["Status"] == "Completed") &
    (store_table["PaymentStatus"] == "Paid"),
    store_table["Revenue"],
    0
)

display(store_table)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue

# COMMAND ----------

total_revenue = store_table["Revenue"].sum().round(2)

display(total_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC - Total Net Revenue

# COMMAND ----------

total_net_revenue = store_table["NetRevenue"].sum()

display(total_net_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC - Total Realised Orders

# COMMAND ----------

total_orders = store_table[
    store_table["NetRevenue"] > 0
]["OrderID"].nunique()

display(total_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC - Average Order Value

# COMMAND ----------

average_order_value = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("OrderID")["NetRevenue"]
    .sum()
    .mean()
).round(2)

display(average_order_value)

# COMMAND ----------

# MAGIC %md
# MAGIC - Total Units Sold

# COMMAND ----------

total_units_sold = store_table[
    store_table["NetRevenue"] > 0
]["Quantity"].sum()

display(total_units_sold)

# COMMAND ----------

# MAGIC %md
# MAGIC - Completion Rate

# COMMAND ----------

completion_rate = (
    store_table["Status"].eq("Completed").mean() * 100
).round(2)

display(completion_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC - Cancelled + Returned Rate

# COMMAND ----------

cancelled_returned_rate = (
    store_table["Status"]
    .isin(["Cancelled", "Returned"])
    .mean()
    * 100
).round(2)

display(cancelled_returned_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment Failure Rate

# COMMAND ----------

payment_failure_rate = (
    store_table["PaymentStatus"].eq("Failed").mean()
    * 100
).round(2)

display(payment_failure_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC - Average Discount

# COMMAND ----------

average_discount = (
    store_table["Discount"].mean() * 100
).round(2)

display(average_discount)

# COMMAND ----------

# MAGIC %md
# MAGIC - Total Customers

# COMMAND ----------

total_customers = store_table["CustomerID"].nunique()

display(total_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC - Monthly Net Revenue Trend

# COMMAND ----------

monthly_revenue = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby(
        store_table["OrderDate"].dt.to_period("M")
    )["NetRevenue"]
    .sum()
    .reset_index()
)

monthly_revenue["OrderDate"] = (
    monthly_revenue["OrderDate"].dt.to_timestamp()
)

display(monthly_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC - Orders By Month

# COMMAND ----------

monthly_orders = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby(
        store_table["OrderDate"].dt.to_period("M")
    )["OrderID"]
    .nunique()
    .reset_index(name="Orders")
)

monthly_orders["OrderDate"] = (
    monthly_orders["OrderDate"].dt.to_timestamp()
)

display(monthly_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC - Monthly Average Order Value(AOV)

# COMMAND ----------

monthly_aov = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby(
        [
            store_table["OrderDate"].dt.to_period("M"),
            "OrderID"
        ]
    )["NetRevenue"]
    .sum()
    .groupby(level=0)
    .mean()
    .reset_index(name="AOV")
).round(2)

monthly_aov["OrderDate"] = (
    monthly_aov["OrderDate"].dt.to_timestamp()
)

display(monthly_aov)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue By Category

# COMMAND ----------

category_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("Category")
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        UnitsSold=("Quantity", "sum"),
        Orders=("OrderID", "nunique")
    )
    .sort_values(
        "NetRevenue",
        ascending=False
    )
    .reset_index()
)

display(category_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Units Sold By Category

# COMMAND ----------

category_units = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("Category")["Quantity"]
    .sum()
    .sort_values(ascending=False)
    .reset_index(name="UnitsSold")
)

display(category_units)

# COMMAND ----------

# MAGIC %md
# MAGIC - Top 10 Products By Revenue

# COMMAND ----------

top_products_revenue = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("ProductName")
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        UnitsSold=("Quantity", "sum"),
        Orders=("OrderID", "nunique")
    )
    .sort_values(
        "NetRevenue",
        ascending=False
    )
    .head(10)
    .reset_index()
).round(2)

display(top_products_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC - Top 10 Products by Unit Sold

# COMMAND ----------

top_products_by_units_sold = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("ProductName")["Quantity"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
    .reset_index(name="UnitsSold")
)

display(top_products_by_units_sold)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue By City

# COMMAND ----------

city_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("City")
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        Orders=("OrderID", "nunique"),
        AOV=("NetRevenue", "mean")
    )
    .sort_values(
        "NetRevenue",
        ascending=False
    )
    .reset_index()
).round(2)

display(city_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue By Customer Segments

# COMMAND ----------

segment_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("CustomerSegment")
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        Orders=("OrderID", "nunique"),
        AOV=("NetRevenue", "mean")
    )
    .sort_values(
        "NetRevenue",
        ascending=False
    )
    .reset_index()
).round(2)

display(segment_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue By Age Bucket

# COMMAND ----------

age_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("AgeBucket")
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        Orders=("OrderID", "nunique")
    )
    .sort_values(
        "NetRevenue",
        ascending=False
    )
    .reset_index()
).round(2)

display(age_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Order Status Analysis

# COMMAND ----------

status_analysis = (
    store_table
    .groupby("Status")
    .agg(
        Orders=("OrderID", "nunique"),
        Revenue=("Revenue", "sum")
    )
    .sort_values(
        "Orders",
        ascending=False
    )
    .reset_index()
)

display(status_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment Status Analysis

# COMMAND ----------

payment_status_analysis = (
    store_table
    .groupby("PaymentStatus")
    .agg(
        Orders=("OrderID", "nunique"),
        Revenue=("Revenue", "sum")
    )
    .sort_values(
        "Orders",
        ascending=False
    )
    .reset_index()
)

display(payment_status_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Payments Method Analysis

# COMMAND ----------

payment_method_analysis = (
    store_table
    .groupby("PaymentMethod")
    .agg(
        Orders=("OrderID", "nunique"),
        NetRevenue=("NetRevenue", "sum"),
        FailedPayments=(
            "PaymentStatus",
            lambda x: (x == "Failed").sum()
        )
    )
    .reset_index()
).round(2)

payment_method_analysis["FailureRate"] = (
    payment_method_analysis["FailedPayments"]
    / payment_method_analysis["Orders"]
    * 100
).round(2)

display(payment_method_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Discount Analysis

# COMMAND ----------

discount_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("Discount")
    .agg(
        Orders=("OrderID", "nunique"),
        AverageQuantity=("Quantity", "mean"),
        AverageRevenue=("Revenue", "mean"),
        TotalRevenue=("Revenue", "sum")
    )
    .reset_index()
    .sort_values("Discount")
).round(2)

display(discount_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Discount vs Quantity

# COMMAND ----------

discount_quantity = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("Discount")["Quantity"]
    .mean()
    .reset_index(name="AverageQuantity")
    .sort_values("Discount")
).round(2)

display(discount_quantity)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue by Day Classification

# COMMAND ----------

day_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("day_classification_x")
    .agg(
        Orders=("OrderID", "nunique"),
        NetRevenue=("NetRevenue", "sum")
    )
    .reset_index()
)

day_analysis["AOV"] = (
    day_analysis["NetRevenue"]
    / day_analysis["Orders"]
).round(2)

display(day_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue by Day-of-Week

# COMMAND ----------

day_name_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("DayName_x")
    .agg(
        Orders=("OrderID", "nunique"),
        NetRevenue=("NetRevenue", "sum")
    )
    .reindex([
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday"
    ])
    .reset_index()
)

display(day_name_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue by Quater

# COMMAND ----------

quarter_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("Quarter_x")
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        Orders=("OrderID", "nunique")
    )
    .reset_index()
)

display(quarter_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue by Year

# COMMAND ----------

year_analysis = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby("Year_x")
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        Orders=("OrderID", "nunique")
    )
    .reset_index()
)

display(year_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue Year Over Year 

# COMMAND ----------

year_analysis["YoY_Growth"] = (
    year_analysis["NetRevenue"]
    .pct_change()
    * 100
).round(2)

display(year_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Revenue v Orders

# COMMAND ----------

monthly_performance = (
    store_table[store_table["NetRevenue"] > 0]
    .groupby(
        store_table["OrderDate"].dt.to_period("M")
    )
    .agg(
        NetRevenue=("NetRevenue", "sum"),
        Orders=("OrderID", "nunique")
    )
    .reset_index()
).round(2)

monthly_performance["AOV"] = (
    monthly_performance["NetRevenue"]
    / monthly_performance["Orders"]
).round(2)

monthly_performance["OrderGrowth"] = (
    monthly_performance["Orders"]
    .pct_change()
    * 100
).round(2)

monthly_performance["AOVGrowth"] = (
    monthly_performance["AOV"]
    .pct_change()
    * 100
).round(2)

monthly_performance["RevenueGrowth"] = (
    monthly_performance["NetRevenue"]
    .pct_change()
    * 100
).round(2)

display(monthly_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC