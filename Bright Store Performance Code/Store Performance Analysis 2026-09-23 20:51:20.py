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

# Checking the Size of the Products Dataset
# This code displays the number of rows and columns in the products DataFrame.
display(products.shape)

# COMMAND ----------

# Checking the Structure of the Products DataFtrame.
# This code provides a summary of the structure of the products DataFrame, including its columns, number of non-null values, and data types.
products.info()

# COMMAND ----------

# Generating Descriptive Statistics
# This code generates descriptive statistics for the numerical columns in the products DataFrame, including the count, average, standard deviation, minimum, quartiles and maximum values. The .round(2) rounds the results to two decimal places, making the statistics easier to read.
products.describe().round(2)

# COMMAND ----------

# This code displays first 10 rows of the DataFrame.
display(products.head(10))

# COMMAND ----------

# This code displays last 10 rows of the DataFrame.
display(products.tail(10))

# COMMAND ----------

# Checking for Nulls and Empty Values
# This code checks each column in the products DataFrame for null or missing values and counts how many null values are present in each column. It is useful for identifying columns that have missing data before cleaning or analysing the dataset.

products.isna().sum()

# COMMAND ----------

# Checking for duplicates
products.duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - ProductID Column

# COMMAND ----------

# This counts the number of non-null values in the ProductID column of the products DataFrame. It determines how many product records actually have a ProductID present. 
products["ProductID"].count()

# COMMAND ----------

# This counts how many times each individual ProductID appears in the products DataFrame. It returns each unique ProductID together with its frequency, which allows you to check whether ProductIDs are unique or whether any ProductID appears more than once. This is particularly useful for checking whether ProductID can function as a reliable identifier for each product.
products["ProductID"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Category Column

# COMMAND ----------

# This counts the number of unique categories in the Category column. It tells you how many different product categories exist in the products DataFrame, without counting repeated occurrences of the same category.
products["Category"].nunique()

# COMMAND ----------

# This counts how many products belong to each category. The result lists each unique category and the number of times it appears in the Category column, allowing you to see how the products are distributed across categories.
products["Category"].value_counts()

# COMMAND ----------

# This identifies the product with the highest UnitPrice and the product with the lowest UnitPrice within each category. The groupby("Category") separates the products by category, while idxmax and idxmin find the row positions of the maximum and minimum prices in each category. .stack() reshapes those results so they can be used to select the corresponding rows with .loc[], and .sort_values() then organizes the output by Category and UnitPrice. Finally, display() shows the selected products in Databricks.
display(products.loc[
    products.groupby("Category")["UnitPrice"].agg(["idxmax", "idxmin"]).stack()
].sort_values(["Category", "UnitPrice"]))

# COMMAND ----------

# MAGIC %md
# MAGIC - Unit Price

# COMMAND ----------

# This code adds together all the values in the UnitPrice column and returns the total of all product unit prices in the products DataFrame. It gives you the combined price values of all products, rather than the average price of a product.
products["UnitPrice"].sum()

# COMMAND ----------

# This groups the products by Category and calculates two measures for each category: Category_Count counts the number of products using the ProductName column, while Total_Unit_Price adds together the UnitPrice values within that category. The results are then sorted from the category with the highest total unit price to the lowest, reset_index() converts Category back into a normal column, and display() shows the final table.
display(
    products.groupby("Category").agg(
        Category_Count=("ProductName", "count"),
        Total_Unit_Price=("UnitPrice", "sum")
    ).sort_values("Total_Unit_Price", ascending=False).reset_index()
)

# COMMAND ----------

# This groups the products by Category and calculates the average (Mean), lowest (Minimum), and highest (Maximum) UnitPrice for each category. .round(2) rounds the results to two decimal places, while .reset_index() turns Category back into a normal column so the result can be displayed as a clean table.
display(products.groupby("Category")["UnitPrice"].agg(
    Mean="mean",
    Minimum="min",
    Maximum="max"
).round(2).reset_index())

# COMMAND ----------

# This calculates four overall statistics for the UnitPrice column: the average price (Mean), the lowest price (Minimum), the highest price (Maximum), and the number of non-null prices (Count). The results are rounded to two decimal places and sorted from the largest value to the smallest.
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

# This code displays the complete orders DataFrame.
display(orders)

# COMMAND ----------

# This provides a summary of the orders DataFrame.
orders.info()

# COMMAND ----------

# This converts OrderDate into a proper datetime data type, allowing you to perform date-based analysis such as extracting the year, month, day, or calculating date ranges. It also converts Quantity to Pandas' nullable integer type Int64, which allows the column to contain whole numbers while still supporting missing values.
orders["OrderDate"] = pd.to_datetime(orders["OrderDate"])
orders["Quantity"] = orders["Quantity"].astype("Int64")

# COMMAND ----------

# This code confirms that the columns, particularly OrderDate and Quantity, have been converted to the intended data types.
display(orders.dtypes)

# COMMAND ----------

# This code identifies the earliest and latest order dates in the OrderDate column. .min() finds the earliest date and .max() finds the latest date, allowing me to determine the time period covered by the orders dataset. 
start_date = orders["OrderDate"].min()
end_date = orders["OrderDate"].max()

display("Start Date:", start_date)
display("End Date:", end_date)

# COMMAND ----------

# This code returns the number of rows and columns in the orders DataFrame. The result is shown as (rows, columns), which gives me a quick overview of the size of the dataset.
display(orders.shape)

# COMMAND ----------

# This displays the first 10 rows of the orders DataFrame.
display(orders.head(10))

# COMMAND ----------

# This displays the last 10 rows of the orders DataFrame.
display(orders.tail(10))

# COMMAND ----------

# This code generates descriptive statistics for the numeric columns in the orders DataFrame, such as count, mean, standard deviation, minimum, maximum, and quartile values. .round(2) rounds the results to two decimal places, making the output easier to read.
orders.describe().round(2)

# COMMAND ----------

# This code checks every column in the orders DataFrame for missing/null values. 
display(orders.isna().sum())

# COMMAND ----------

# This code checks the orders DataFrame for exact duplicate rows. .duplicated() identifies rows that completely repeat a previous row, while .sum() counts those duplicates.
display(orders.duplicated().sum())

# COMMAND ----------

# This code checks each column individually and counts how many repeated values occur within each column. The lambda col applies the duplicate check to every column, and .sum() counts the duplicated values.
display(
    orders.apply(lambda col: col.duplicated().sum())
)

# COMMAND ----------

# MAGIC %md
# MAGIC -

# COMMAND ----------

# MAGIC %md
# MAGIC - Order ID Column

# COMMAND ----------

# This code displays all rows where the OrderID appears more than once. duplicated(keep=False) marks every occurrence of a duplicated OrderID as True, rather than keeping the first occurrence as unique. The matching rows are then sorted by OrderID so you can inspect the duplicate order records together.
display(
    orders[orders["OrderID"].duplicated(keep=False)]
    .sort_values("OrderID")
)

# COMMAND ----------

# This counts how many times each OrderID appears and then keeps only the IDs that appear more than once. .value_counts() calculates the frequency of each OrderID, .loc[lambda x: x > 1] filters the results to duplicated IDs, and .reset_index() converts the result into a DataFrame with a Duplicate Count column. The final rename() keeps the identifier column clearly labelled as OrderID.
display(
    orders["OrderID"]
    .value_counts()
    .loc[lambda x: x > 1]
    .reset_index(name="Duplicate Count")
    .rename(columns={"OrderID": "OrderID"})
)

# COMMAND ----------

# This code removes repeated OrderID values from the orders DataFrame and keeps only the first occurrence of each OrderID. This means that if an OrderID appears multiple times, all occurrences after the first one are removed.
orders = orders.drop_duplicates(
    subset=["OrderID"],
    keep="first"
)

# COMMAND ----------

# Checking if duplicates were removed successfully under the OrderID column after the previous cleaning step.
display(
    orders.apply(lambda col: col.duplicated().sum())
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Customer ID Column

# COMMAND ----------

# This code counts how many times each CustomerID appears in the orders DataFrame and displays only customers who appear more than once. This is useful for identifying customers with multiple orders.
display(
    orders["CustomerID"]
    .value_counts()
    .loc[lambda x: x > 1]
    .reset_index(name="Duplicate Count")
    .rename(columns={"CustomerID": "CustomerID"})
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Order Date Column

# COMMAND ----------

# This identifies the most frequently occurring date in the OrderDate column. The .mode() function returns the value or values that appear most often. 
# This step is for understanding which order date occurs most frequently without changing or removing any data.
orders["OrderDate"].mode()

# COMMAND ----------

# This replaces missing values in the OrderDate column with the most frequently occurring date. mode() finds the date that appears most often, [0] selects the first mode if there is more than one, and fillna() uses that date wherever OrderDate is missing. The cleaned dates are then assigned back to the OrderDate column.
orders["OrderDate"] = orders["OrderDate"].fillna(orders["OrderDate"].mode()[0])

# COMMAND ----------

# This creates new columns from the OrderDate column so the dates can be analysed more easily. Year extracts the year, MonthName extracts the month name, Day extracts the day number, DayName extracts the day of the week, and Quarter identifies the quarter of the year.
orders["Year"] = orders["OrderDate"].dt.year
orders["MonthName"] = orders["OrderDate"].dt.month_name()
orders["Day"] = orders["OrderDate"].dt.day
orders["DayName"] = orders["OrderDate"].dt.day_name()
orders["Quarter"] = orders["OrderDate"].dt.quarter

display(orders)

# COMMAND ----------

# This creates a day_classification column that categorises each order as either Weekday or Weekend based on OrderDate. Pandas represents Monday as 0 through Friday as 4, so the condition < 5 identifies weekdays; if the condition is true, "Weekday" is assigned, otherwise "Weekend" is assigned.
orders["day_classification"] = np.where(
    orders["OrderDate"].dt.dayofweek < 5,
    "Weekday",
    "Weekend"
)

display(orders)

# COMMAND ----------

# This code creates a Season column by extracting the month number from OrderDate and mapping each month to its corresponding South African season. December, January and February are classified as Summer; March, April and May as Autumn; June, July and August as Winter; and September, October and November as Spring.
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

# This counts how many times each ProductID appears in the orders DataFrame. It helps you understand how frequently each product occurs in the order data and can also help identify products that appear unusually often or rarely.
orders["ProductID"].value_counts()

# COMMAND ----------

# This checks whether every ProductID in the orders DataFrame also exists in the products DataFrame. .isin() returns True when an order's ProductID is found in the products table and False when it is not found. .value_counts() then counts the number of True and False results, allowing you to identify whether there are any unmatched ProductIDs between the two tables.
orders["ProductID"].isin(products["ProductID"]).value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Quantity Column

# COMMAND ----------

# This checks the data type of the Quantity column. It allows you to confirm whether Quantity is stored as an appropriate numeric/integer type for calculations and analysis.
orders["Quantity"].dtypes


# COMMAND ----------

# This returns the number of values in the Quantity Series. Because Quantity is a single column, the result represents the number of rows in that column.
orders["Quantity"].shape

# COMMAND ----------

# This counts the number of missing/null values in the Quantity column. isnull() identifies missing values as True, and sum() counts those True values.
orders["Quantity"].isnull().sum()


# COMMAND ----------

# This code calculates the percentage of Quantity values that are missing. isnull() creates a True/False result, .mean() calculates the proportion of missing values because True is treated as 1 and False as 0, and multiplying by 100 converts that proportion into a percentage.
orders["Quantity"].isnull().mean() * 100

# COMMAND ----------

# This code adds together all the values in the Quantity column and returns the total number of units represented by the orders.
orders["Quantity"].sum()

# COMMAND ----------

# This code counts how many times each individual quantity value appears. For example, it can show how many orders had a quantity of 1, 2, 3, and so on, helping you understand the distribution of quantities.
orders["Quantity"].value_counts()

# COMMAND ----------

# This code calculates the percentage distribution of each Quantity value. normalize=True changes the counts into proportions, multiplying by 100 converts them into percentages, and .round(2) rounds the percentages to two decimal places.
(orders["Quantity"].value_counts(normalize=True) * 100).round(2)

# COMMAND ----------

# This code generates descriptive statistics for the Quantity column, including count, mean, standard deviation, minimum, 25th percentile, median (50th percentile), 75th percentile, and maximum. .round(2) rounds the numerical results to two decimal places.
orders["Quantity"].describe().round(2)


# COMMAND ----------

# This code calculates four statistical measures for Quantity: mean for the average quantity, median for the middle value, std for the standard deviation showing variation, and skew for the direction and degree of asymmetry in the distribution. The results are rounded to two decimal places.
orders["Quantity"].agg(["mean", "median", "std", "skew"]).round(2)


# COMMAND ----------

# This code identifies the most frequently occurring Quantity value in the column. The mode is useful for determining the quantity that appears most often among the orders.
orders["Quantity"].mode()

# COMMAND ----------

# This code performs three checks on the Quantity column: the first line counts the number of missing Quantity values, the second calculates the mean Quantity and rounds it to two decimal places, and the third calculates the median Quantity. Together, these results help you understand missingness and compare the average with the middle value before deciding how missing quantities should be handled.
orders["Quantity"].isna().sum()
orders["Quantity"].mean().round(2)
orders["Quantity"].median()


# COMMAND ----------

# This code fills missing Quantity values using the median Quantity for the corresponding ProductID. groupby("ProductID") separates the orders by product, transform("median") calculates the median quantity for each product while keeping the original row structure, and fillna() uses those product-specific medians to replace missing Quantity values. This is more targeted than using one overall median for every product.
orders["Quantity"] = orders["Quantity"].fillna(orders.groupby("ProductID")["Quantity"].transform("median"))

# COMMAND ----------

# This code checks the entire orders DataFrame for missing values after the Quantity cleaning step. It counts the remaining null values in every column, allowing you to see whether missing Quantity values were successfully addressed and whether other columns still contain missing data.
orders.isna().sum()

# COMMAND ----------

# This code filters the orders DataFrame and displays records where Quantity is zero or negative. The purpose is to identify values that may not make sense for normal product sales and require further investigation before being used in analysis.
orders[orders["Quantity"] <= 0]

# COMMAND ----------

# This code first identifies Quantity values that are zero or negative and replaces them with NaN, treating them as missing/invalid values. The second line then fills those missing values using the median Quantity for the relevant ProductID, meaning the replacement is based on the typical quantity for that particular product.
orders.loc[orders["Quantity"] <= 0, "Quantity"] = np.nan
orders["Quantity"] = orders["Quantity"].fillna(orders.groupby("ProductID")["Quantity"].transform("median"))

# COMMAND ----------

# This code counts the frequency of each Quantity value after the cleaning process. It allows you to check how the Quantity values are distributed and whether the invalid or missing values have been handled as expected.
orders["Quantity"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Discount Column

# COMMAND ----------

# This code checks the data type of the Discount column. It helps confirm whether Discount is stored in an appropriate format for numerical analysis and calculations.
orders["Discount"].dtype

# COMMAND ----------

# This code counts the number of missing/null values in the Discount column. It helps determine how much missing discount data needs to be addressed during cleaning.
orders["Discount"].isna().sum()


# COMMAND ----------

# This code counts how frequently each Discount value occurs, including missing values because dropna=False tells Pandas not to exclude them. .sort_index() then arranges the discount values in ascending order, making the distribution easier to inspect.
orders["Discount"].value_counts(dropna=False).sort_index()

# COMMAND ----------

# This code generates descriptive statistics for the Discount column, including the count, mean, standard deviation, minimum, quartiles, and maximum. The results are rounded to two decimal places to make them easier to read.
orders["Discount"].describe().round(2)

# COMMAND ----------

# This code finds the most frequently occurring Discount value. mode() returns the most common value or values, and [0] selects the first value from the result.
orders["Discount"].mode()[0]

# COMMAND ----------

# This code calculates the skewness of the Discount distribution and rounds the result to two decimal places. Skewness helps you understand whether the discount values are distributed symmetrically or whether they are concentrated more toward one side.
orders["Discount"].skew().round(2) 

# COMMAND ----------

# This code calculates the percentage distribution of each Discount value. normalize=True converts the frequency counts into proportions, .sort_index() orders the discounts from lowest to highest, .round(2) rounds the proportions, and * 100 converts them into percentages.
orders["Discount"].value_counts(normalize=True).sort_index().round(2) * 100

# COMMAND ----------

# This code groups the orders according to their Quantity and calculates the median Discount for each quantity level. This allows you to investigate whether the typical discount changes depending on how many units were purchased.
orders.groupby("Quantity")["Discount"].median()

# COMMAND ----------

# This code replaces missing Discount values with the overall median Discount from the column. Using the median provides a central value that is less affected by unusually high or low discounts than the mean.
orders["Discount"] = orders["Discount"].fillna(orders["Discount"].median())

# COMMAND ----------

# This code counts the remaining missing values in the Discount column after the filling step. A result of 0 means there are no longer any missing Discount values.
orders["Discount"].isna().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment Method Column

# COMMAND ----------

# This code checks the data type of the PaymentMethod column. Since payment methods are normally categories or text values, this allows you to confirm how the column is currently stored.
orders["PaymentMethod"].info()

# COMMAND ----------

# This code counts the number of missing/null values in the PaymentMethod column. It tells you how many orders do not currently have a recorded payment method and helps determine whether further cleaning is required.
orders["PaymentMethod"].isna().sum()

# COMMAND ----------

# This counts how many times each payment method appears in the PaymentMethod column. It shows the frequency of each payment method, allowing you to see which payment methods are used most and least often.
orders["PaymentMethod"].value_counts()

# COMMAND ----------

# This code calculates the percentage distribution of payment methods. normalize=True converts the counts into proportions, .round(2) rounds the proportions to two decimal places, and * 100 converts them into percentages.
orders["PaymentMethod"].value_counts(normalize=True).round(2) * 100

# COMMAND ----------

# This code replaces missing values in the PaymentMethod column with the category "Unknown". Because PaymentMethod is a categorical field, using "Unknown" preserves those records instead of removing them and clearly identifies orders where the payment method was not provided.
orders["PaymentMethod"] = orders["PaymentMethod"].fillna("Unknown")

# COMMAND ----------

# This code checks every column in the orders DataFrame and counts the remaining missing values in each column. It allows you to confirm whether the missing PaymentMethod values were successfully replaced and identify any missing values still requiring attention elsewhere.
orders.isna().sum()

# COMMAND ----------

# This code counts the occurrences of each payment method again after replacing the missing values with "Unknown". It allows you to confirm that "Unknown" has been added as a category and see the updated distribution of payment methods.
orders["PaymentMethod"].value_counts()


# COMMAND ----------

# MAGIC %md
# MAGIC - Status Column

# COMMAND ----------

# This code counts the number of missing values in the Status column. It tells you whether any orders are missing their status information.
orders["Status"].isna().sum()

# COMMAND ----------

# This code counts how many orders belong to each status category. It allows you to see the frequency of statuses such as Completed, Cancelled, or Returned in the orders data.
orders["Status"].value_counts()

# COMMAND ----------

# This code calculates the percentage distribution of each order status. normalize=True converts the counts into proportions, .round(2) rounds them to two decimal places, and multiplying by 100 converts them into percentages.
orders["Status"].value_counts(normalize=True).round(2) * 100

# COMMAND ----------

# This code creates a cross-tabulation showing the proportion of each order status for every ProductID. ProductID forms the rows and Status forms the columns, while normalize="index" calculates the percentages within each product so you can compare the proportion of Completed, Cancelled, and Returned orders for each product.
pd.crosstab(orders["ProductID"], orders["Status"], normalize="index").round(2)

# COMMAND ----------

# This code checks whether each ProductID in the orders DataFrame exists in the products DataFrame. The result counts True values for matching ProductIDs and False values for ProductIDs that cannot be found in the products table, helping me to identify broken product relationships before joining the tables.
orders["ProductID"].isin(products["ProductID"]).value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ##JOINING ORDERS TABLE WITH PRODUCTS TABLE

# COMMAND ----------

# This code combines the orders and products DataFrames using ProductID as the common key. A left join keeps every record from the orders DataFrame and adds the matching product information from products, such as ProductName, Category, and UnitPrice. The result is stored in a new DataFrame called orders_products.
orders_products = orders.merge(
    products,
    on="ProductID",
    how="left"
)

# COMMAND ----------

# This code displays the newly joined orders_products DataFrame.
display(orders_products)

# COMMAND ----------

# This code displays the number of rows and columns in the joined orders_products DataFrame. Checking the shape after a join helps you confirm whether the number of records changed unexpectedly and whether the expected product columns were added.
display(orders_products.shape)

# COMMAND ----------

# This provides information about the joined DataFrame, including its number of rows, columns, non-null values, and data types. 
# This is to verify the structure of the data after combining the two tables.
orders_products.info()

# COMMAND ----------

# This code counts the missing values in every column of the joined DataFrame. It is particularly useful after a left join because missing product information could indicate that an OrderID contains a ProductID that was not found in the products table.
orders_products.isna().sum()

# COMMAND ----------

# This code counts the number of exactly duplicated rows in the joined orders_products DataFrame. 
# This is to determine whether the join or the original data has created repeated records.
orders_products.duplicated().sum()

# COMMAND ----------

# This code generates descriptive statistics for the numeric columns in orders_products, including values such as count, mean, standard deviation, minimum, quartiles, and maximum. The results are rounded to two decimal places.
orders_products.describe().round(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #3. CUSTOMERS DataFrame EDA

# COMMAND ----------

# This code displays the complete customers DataFrame for inspection before performing further analysis or cleaning.
display(customers)

# COMMAND ----------

# This code returns the number of rows and columns in the customers DataFrame. It gives you a quick understanding of the size and structure of the customer dataset.
display(customers.shape)

# COMMAND ----------

# This code provides a summary of the customers DataFrame, including the column names, number of non-null values, data types, and memory information. 
# This is to identify columns such as Age and SignupDate that may need data-type conversion.
customers.info()

# COMMAND ----------

# This code counts the number of missing values in each column of the customers DataFrame. 
# This is to identify which customer fields have incomplete information.
customers.isna().sum()

# COMMAND ----------

# This code counts the number of exact duplicate rows in the customers DataFrame. It helps determine whether identical customer records have been repeated.
customers.duplicated().sum()

# COMMAND ----------

# This code generates descriptive statistics for the numeric columns in the customers DataFrame, such as count, mean, standard deviation, minimum, quartiles, and maximum. 
# The results are rounded to two decimal places.
customers.describe().round(2)

# COMMAND ----------

# MAGIC %md
# MAGIC - CustomerID Column

# COMMAND ----------

# This code counts the number of non-null CustomerID values in the customers DataFrame. This tells how many customer records have a CustomerID recorded.
customers["CustomerID"].count()

# COMMAND ----------

# This code counts the number of missing CustomerID values. Since CustomerID is intended to identify individual customers, this check helps determine whether any customer records lack an identifier.
customers["CustomerID"].isna().sum()

# COMMAND ----------

# This code counts how many CustomerID values are repeated within the customers DataFrame. If CustomerID is intended to be a primary key, you would normally expect each customer to have a unique ID.
customers["CustomerID"].duplicated().sum()

# COMMAND ----------

# This code checks whether the CustomerID values in orders_products can be found in the customers table. True represents a CustomerID that exists in the customers table, while False represents a CustomerID with no matching customer record. 
# This is to identify broken customer relationships before joining the tables.
orders_products["CustomerID"].isin(customers["CustomerID"]).value_counts()

# COMMAND ----------

# This code checks whether every CustomerID appears only once in the customers DataFrame. A result of True means that all CustomerIDs are unique, which supports using CustomerID as a primary key for the customers table.
customers["CustomerID"].is_unique

# COMMAND ----------

# This code calculates the smallest and largest CustomerID values in the customers DataFrame.
# CustomerID column is a clean primary key.
# 100001 to 110000 is exactly 10,000 numbers, matching the row count.
customers["CustomerID"].agg(["min", "max"])

# COMMAND ----------

# This code checks whether each CustomerID increases by exactly 1 from the previous CustomerID. diff() calculates the difference between consecutive IDs, dropna() removes the first missing difference, .eq(1) checks whether every difference equals 1, and .all() returns True only if all consecutive CustomerIDs increase by exactly one. 
# This is for validation of a sequential CustomerID range.
customers["CustomerID"].diff().dropna().eq(1).all()

# COMMAND ----------

# MAGIC %md
# MAGIC - Age Column

# COMMAND ----------

# This code displays a summary of the bcustomers DataFrame.
customers.info()

# COMMAND ----------

# This code converts the Age column to Pandas' nullable integer type Int64. This allows ages to be stored as whole numbers while still allowing missing values to exist in the column.
customers["Age"] = customers["Age"].astype("Int64")

# COMMAND ----------

# This code displays information about the Age Series, including its data type and the number of non-null values.
# This is to confirm that the Age column has the expected structure after conversion.
customers["Age"].info()

# COMMAND ----------

# This code counts the number of non-null values in the Age column. 
# It shows how many customer records have an age recorded.
customers["Age"].count()

# COMMAND ----------

# This code counts how many customers have each specific age. This is to understand how the customers are distributed across different ages.
display(customers["Age"].value_counts())

# COMMAND ----------

# This code counts the number of customer records where the Age value is missing.
customers["Age"].isna().sum()

# COMMAND ----------

# This code generates descriptive statistics for Age, including the count, mean, standard deviation, minimum, quartiles, and maximum. .round(0) rounds the results to zero decimal places because age is measured in whole years.
customers["Age"].describe().round(0)

# COMMAND ----------

# This code identifies the age or ages that occur most frequently in the customer data.
customers["Age"].mode()

# COMMAND ----------

# This code calculates selected percentiles of the Age distribution. It returns the 5th percentile, 25th percentile, 50th percentile (median), 75th percentile, and 95th percentile, helping you understand how customer ages are distributed across the dataset.
customers["Age"].quantile([.05, .25, .5, .75, .95])

# COMMAND ----------

# This code divides customer ages into predefined ranges: 18–24, 25–34, 35–44, 45–54, and 55–65, and then counts how many customers fall into each range. pd.cut() creates the age intervals, while value_counts() counts the customers within each interval.
pd.cut(customers["Age"], [17, 24, 34, 44, 54, 65]).value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Why i used 'MEDIAN' in the Age Column?
# MAGIC - I used median to fill missing Age values because the median represents the middle of the customer age distribution and is less affected by extreme ages. 
# MAGIC - In this dataset, both the mean and median age were 41, so using the median produced the same central value while providing a more robust approach to missing-value treatment. 
# MAGIC - This allowed me to retain the customer records and consistently classify customers into age groups.

# COMMAND ----------

# This code replaces missing Age values with the median age of the customer dataset. In your analysis, the median was 41, so missing ages are replaced with 41 rather than being removed from the dataset.
customers["Age"] = customers["Age"].fillna(customers["Age"].median())

# COMMAND ----------

# This code checks how many missing values remain in the Age column after replacing them with the median. A result of 0 means there are no remaining missing Age values.
customers["Age"].isna().sum()

# COMMAND ----------

# This code counts the frequency of each age again after the missing values have been filled. It allows you to see the updated distribution and confirm that the replacement value has been incorporated.
display(customers["Age"].value_counts())

# COMMAND ----------

# This code creates a function called age_bucket() that classifies each customer's age into four groups: Youth (18–24), Young Adults (25–34), Adults (35–54), and Seniors (55+). .apply(age_bucket) applies the function to every value in the Age column and creates a new AgeBucket column. 
# The final display() shows the customers DataFrame with the new classification.
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

# This code counts how many customers belong to each AgeBucket. reset_index() converts the resulting Series into a DataFrame structure, making the result easier to display and use for further analysis.
display(
    customers["AgeBucket"].value_counts().reset_index()
)

# COMMAND ----------

# This code displays the complete customers DataFrame after the Age and AgeBucket transformations, to inspect the updated customer records.
display(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC - City Column

# COMMAND ----------

# This code counts the number of customer records where the City value is missing.
customers["City"].isna().sum()

# COMMAND ----------

# This code replaces missing City values with "Unknown". This keeps the customer records in the dataset while clearly identifying that the customer's city was not provided.
customers["City"] = customers["City"].fillna("Unknown")

# COMMAND ----------

# This code checks whether any missing City values remain after the replacement. A result of 0 means all missing City values have been handled.
customers["City"].isna().sum()

# COMMAND ----------

# This code counts the number of different city values in the City column. It tells you how many unique city categories exist.
customers["City"].nunique()

# COMMAND ----------

# This code counts how many customers are associated with each city. 
# It is to understand the geographic distribution of the customer base.
customers["City"].value_counts()

# COMMAND ----------

# This code standardises the City values by removing unnecessary spaces with .str.strip(), converting city names into title case with .str.title(), and correcting the spelling "Mashad" to "Mashhad". 
# The purpose is to make differently formatted or misspelled city values consistent for analysis.
customers["City"] = (customers["City"]
    .str.strip()
    .str.title()
    .replace({"Mashad": "Mashhad"}))


# COMMAND ----------

# This code counts the city values again after cleaning them. 
# This is to see the corrected distribution and confirm that inconsistent city names have been standardised.
customers["City"].value_counts()

# COMMAND ----------

# This code recounts the number of unique cities after the cleaning process. 
customers["City"].nunique()

# COMMAND ----------

# MAGIC %md
# MAGIC - Signup Date Column

# COMMAND ----------

# This displays information about the SignupDate column, including its data type and number of non-null values.
# This is to determine whether the column is currently stored in an appropriate date format.
customers["SignupDate"].info()

# COMMAND ----------

# This code converts the SignupDate column into Pandas' datetime format. 
# This to perform date-based operations such as extracting years, months, days, and quarters.
customers["SignupDate"] = pd.to_datetime(customers["SignupDate"])

display(customers)

# COMMAND ----------

# This code finds the earliest and latest customer signup dates. .min() returns the earliest date and .max() returns the latest date, allowing you to understand the period covered by customer registrations.
start_date = customers["SignupDate"].min()
end_date = customers["SignupDate"].max()

display("Start Date:", start_date)
display("End Date:", end_date)

# COMMAND ----------

# This code creates several new columns from SignupDate: Year extracts the year, MonthName extracts the month name, Day extracts the day number, DayName extracts the day of the week, and Quarter identifies the quarter. The updated customers DataFrame is then displayed.
customers["Year"] = customers["SignupDate"].dt.year
customers["MonthName"] = customers["SignupDate"].dt.month_name()
customers["Day"] = customers["SignupDate"].dt.day
customers["DayName"] = customers["SignupDate"].dt.day_name()
customers["Quarter"] = customers["SignupDate"].dt.quarter

display(customers)

# COMMAND ----------

# This code defines a function that classifies each signup date as either Weekday or Weekend. Monday through Friday have day-of-week values below 5 and are classified as Weekday, while Saturday and Sunday are classified as Weekend. 
# The function is then applied to every SignupDate and the result is stored in day_classification.
def classify_day(date):
    if date.dayofweek < 5:
        return "Weekday"
    else:
        return "Weekend"

customers["day_classification"] = customers["SignupDate"].apply(classify_day)

display(customers)

# COMMAND ----------

# This code converts each SignupDate into a quarter period, counts how many customer signups occurred in each quarter, and sorts the quarters chronologically. It is to see how customer registrations are distributed over time.
customers["SignupDate"].dt.to_period("Q").value_counts().sort_index()

# COMMAND ----------

# This code extracts the day of the week from each SignupDate and counts how many customers signed up on each day. It shows which days had the most and fewest customer registrations.
customers["SignupDate"].dt.day_name().value_counts()

# COMMAND ----------

# This code counts how many customer signups occurred on Weekdays versus Weekends, based on the classification created earlier.
customers["day_classification"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Customer Segment

# COMMAND ----------

# This code counts the number of customers whose CustomerSegment value is missing.
customers["CustomerSegment"].isna().sum()

# COMMAND ----------

# This code counts how many customers belong to each customer segment, allowing you to understand the composition of the customer base.
customers["CustomerSegment"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOINING TABLES

# COMMAND ----------

# This code performs a left join between orders_products and customers using CustomerID. It keeps all records from orders_products and adds matching customer information such as Age, City, CustomerSegment, and SignupDate. 
# The resulting combined DataFrame is stored as ord_prod_cust and then displayed.
ord_prod_cust = orders_products.merge(
    customers,
    on="CustomerID",
    how="left"
)

display(ord_prod_cust)

# COMMAND ----------

# This code returns the number of rows and columns in the newly joined ord_prod_cust DataFrame. It is to check whether the join produced the expected dataset size.
ord_prod_cust.shape

# COMMAND ----------

# This code provides a structural summary of ord_prod_cust, including its columns, data types, row count, and non-null values. 
# It is to check the result of the customer join.
ord_prod_cust.info()

# COMMAND ----------

# This code counts missing values in every column of the joined DataFrame. 
# This is to identify customer records that could not be matched or customer fields that remain unavailable.
ord_prod_cust.isna().sum()

# COMMAND ----------

# This code creates an IsGuest flag that identifies records where CustomerID equals 999999. 
# Those records are marked True, while all other customers are marked False. 
# The second line counts how many records fall into each group, allowing you to identify guest orders separately from registered customers.
ord_prod_cust["IsGuest"] = ord_prod_cust["CustomerID"] == 999999

ord_prod_cust["IsGuest"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC In the above code,the data shows that we have 30 customers with missing data and i have decided to keep those customers for the following reasons:
# MAGIC
# MAGIC - Out of 50,000 orders, 30 (just 0.06%) were placed by guests, meaning customers who did not have an account, so we have no details for them such as age, city or signup date. 
# MAGIC - I chose to keep these orders in the data instead of deleting them because the sales themselves are real: 
# MAGIC - they add up to about 1,991 in sales out of a total of 3,503,500 (0.06%) and 49 items out of 94,172 sold (0.05%), and 28 of the 30 were completed. 
# MAGIC - Removing them would make our totals slightly smaller than the true business figures, and the numbers would no longer match the company's records. 
# MAGIC - The customer details for these orders were left blank instead of being guessed, because inventing an age or a city for someone we know nothing about would make the data less accurate, and the text fields were labelled "Unknown" so they are easy to spot. 
# MAGIC - A "guest" marker was added to these 30 orders so they can be left out whenever we study customer behaviour, such as age or city, without losing them from sales totals.

# COMMAND ----------

# This code first creates the IsGuest flag based on CustomerID == 999999. The for loop then goes through the City, CustomerSegment, and AgeBucket columns and replaces missing values with "Unknown". This keeps guest records in the dataset while clearly identifying customer attributes that are unavailable rather than inventing values for them. The resulting DataFrame is then displayed.
ord_prod_cust["IsGuest"] = ord_prod_cust["CustomerID"] == 999999

for col in ["City", "CustomerSegment", "AgeBucket"]:
    ord_prod_cust[col] = ord_prod_cust[col].fillna("Unknown")

display(ord_prod_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC #4. PAYMENTS DataFrame EDA

# COMMAND ----------

# This code returns the number of rows and columns in the payments DataFrame, giving you a quick overview of its size.
payments.shape

# COMMAND ----------

# This code displays the structure of the payments DataFrame, including column names, non-null counts, data types, and the number of records.
payments.info()

# COMMAND ----------

# This code counts the number of missing values in each payments column, helping you identify incomplete payment records.
payments.isna().sum()

# COMMAND ----------

# This code counts how many OrderID values are duplicated in the payments table. 
# It is to determine whether multiple payment records are associated with the same order.
payments["OrderID"].duplicated().sum()

# COMMAND ----------

# This code counts the number of unique orders represented in the payments table.
payments["OrderID"].nunique()

# COMMAND ----------

# This code checks whether each OrderID in the combined orders/products/customers table exists in the payments table. True indicates that a matching payment record exists, while False identifies orders with no matching payment record.
ord_prod_cust["OrderID"].isin(payments["OrderID"]).value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment ID Column

# COMMAND ----------

# This code counts the number of missing values in the PaymentID column.
payments["PaymentID"].isna().sum()

# COMMAND ----------

# This code counts how many PaymentID values are duplicated. Since PaymentID is intended to identify individual payment records, duplicate IDs may require investigation.
payments["PaymentID"].duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Order ID Column

# COMMAND ----------

# This code counts the number of payment records that do not have an OrderID.
payments["OrderID"].isna().sum()

# COMMAND ----------

# This code counts how many OrderID values appear more than once in the payments table. Repeated OrderIDs may be legitimate if an order can have multiple payment attempts, so this should be interpreted according to the business meaning of the payments data.
payments["OrderID"].duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Payment Date Column

# COMMAND ----------

# This code displays information about the PaymentDate column, including its data type and number of non-null values, allowing you to determine whether it is ready for date analysis.
payments["PaymentDate"].info()

# COMMAND ----------

# This code converts PaymentDate into Pandas' datetime format so that dates can be compared, filtered, sorted, and used for time-based analysis.
payments["PaymentDate"] = pd.to_datetime(payments["PaymentDate"])

# COMMAND ----------

# This code finds the earliest and latest payment dates in the payments DataFrame, allowing you to determine the time period covered by the payment records.
start_date = payments["PaymentDate"].min()
end_date = payments["PaymentDate"].max()

display("Start Date:", start_date)
display("End Date:", end_date)

# COMMAND ----------

# This code checks whether the exact dates in OrderDate also appear among the dates in PaymentDate. A True means an OrderDate value is also found as a PaymentDate, while False means it is not. However, this does not actually check whether each order's payment date matches its own order date; it only checks whether the date value exists somewhere in the PaymentDate column.
ord_prod_cust["OrderDate"].isin(payments["PaymentDate"]).value_counts()

# COMMAND ----------

# This code displays the structure and data type information for PaymentDate again, allowing you to confirm that the column remains correctly stored as a datetime after conversion.
payments["PaymentDate"].info()

# COMMAND ----------

# This code counts the number of missing PaymentDate values in the payments table, showing whether any payment records lack a recorded payment date.
payments["PaymentDate"].isna().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC - Temporary Table to check both OrderDate and PaymentDate if the dates are exactly the same

# COMMAND ----------

# This code creates a temporary side_by_side table by joining each OrderID from ord_prod_cust with its corresponding PaymentDate and PaymentStatus from payments, allowing the OrderDate and PaymentDate to be compared for the same order. 
# It then creates a Result column that classifies each order as “Both missing” when both dates are null, “Match” when the OrderDate and PaymentDate are exactly the same, or “Different” when the two dates do not match. 
# The code temporarily displays missing values as "(missing)", counts how many records fall into each result category, and finally displays the complete comparison table. 
# The purpose is to verify whether the order date and payment date are exactly the same for each order before building the final analysis table.
side_by_side = ord_prod_cust[["OrderID", "OrderDate"]].merge(
    payments[["OrderID", "PaymentDate", "PaymentStatus"]],
    on="OrderID", how="left", validate="1:1")

side_by_side["Result"] = np.where(
    side_by_side["OrderDate"].isnull() & side_by_side["PaymentDate"].isnull(), "Both missing",
    np.where(side_by_side["OrderDate"] == side_by_side["PaymentDate"], "Match", "Different"))

side_by_side.fillna("(missing)").head(10) 
side_by_side["Result"].value_counts()

display(side_by_side)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOINING TABLES

# COMMAND ----------

# This code creates the final store_table by joining ord_prod_cust with the payments table using OrderID. It uses a left join, which keeps all records from ord_prod_cust while adding the matching PaymentStatus from payments. 
# The validate="1:1" argument confirms that each OrderID is expected to appear only once on each side of the join. 
# The completed store_table is then displayed so that the final combined dataset can be inspected.
store_table = ord_prod_cust.merge(payments[["OrderID", "PaymentStatus"]],
    on="OrderID", 
    how="left", validate="1:1")

display(store_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #5. FINAL ANALYSIS

# COMMAND ----------

# MAGIC %md
# MAGIC - Data Exoploration

# COMMAND ----------

# This code checks the overall structure of store_table.
store_table.shape

# COMMAND ----------

# This code displays detailed information about the store_table DataFrame, including the number of rows and columns, each column name, the number of non-null values in each column, and the data type of each column.
store_table.info()

# COMMAND ----------

# This code converts the Year_y, Day_y, and Quarter_y columns in store_table to Pandas' nullable integer data type, Int64. This ensures that these date-related values are stored as whole numbers while still allowing the columns to contain missing values. 
# The display(store_table) command then displays the updated store_table so the changes can be checked.
store_table["Year_y"]    = store_table["Year_y"].astype("Int64")
store_table["Day_y"]     = store_table["Day_y"].astype("Int64")
store_table["Quarter_y"] = store_table["Quarter_y"].astype("Int64")

display(store_table)

# COMMAND ----------

# MAGIC %md
# MAGIC - NOTE: The 30 nulls below represent guests that we flagged

# COMMAND ----------

# This code checks every column in the store_table DataFrame for missing or null values and counts how many missing values are present in each column.
store_table.isna().sum()

# COMMAND ----------

# This code checks the store_table DataFrame for completely duplicated rows and counts how many duplicate rows exist.
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

# This code creates two new columns in store_table: Revenue calculates the value of each product line by multiplying the quantity sold by the unit price and then applying the discount using (1 - Discount). 
# NetRevenue then uses np.where() to recognise revenue only when the order status is "Completed" and the payment status is "Paid"; when both conditions are met, it uses the calculated Revenue, otherwise it assigns 0. Finally, display(store_table) displays the updated table with the new Revenue and NetRevenue columns.
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
# MAGIC - Overall Total Revenue

# COMMAND ----------

# This code calculates the total revenue from all records in the Revenue column of store_table. The .sum() function adds all the individual revenue values together, while .round(2) rounds the final result to two decimal places so that the amount is suitable for reporting as a monetary value. 
# The display(total_revenue) command then displays the calculated total revenue.
total_revenue = store_table["Revenue"].sum().round(2)

display(total_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC - Total Net Revenue

# COMMAND ----------

# This code calculates the total Net Revenue generated from the transactions in store_table by adding all the values in the NetRevenue column using .sum(). 
# NetRevenue was previously defined to include only transactions that are Completed and Paid. 
# The display(total_net_revenue) command then displays the final total net revenue.
total_net_revenue = store_table["NetRevenue"].sum()

display(total_net_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC - Total Realised Orders

# COMMAND ----------

# This code calculates the total number of realised orders by first filtering store_table to keep only records where NetRevenue is greater than zero, meaning the transaction generated recognised revenue. 
# It then selects the OrderID column and uses .nunique() to count the number of unique orders, ensuring that an order with multiple product lines is counted only once. 
# The display(total_orders) command then displays the total number of realised orders.
total_orders = store_table[
    store_table["NetRevenue"] > 0
]["OrderID"].nunique()

display(total_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC - Average Order Value

# COMMAND ----------

# This code calculates the Average Order Value (AOV) by first filtering store_table to include only orders with NetRevenue greater than zero, meaning they generated recognised revenue. It then groups the records by OrderID and uses .sum() to calculate the total net revenue for each individual order, which is important because one order can contain multiple product lines. 
# The .mean() then calculates the average value across all realised orders, and .round(2) rounds the result to two decimal places. Finally, display(average_order_value) displays the calculated average order value.
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

# This code calculates the total number of units sold from transactions that generated recognised revenue. It first filters store_table to include only records where NetRevenue is greater than zero, then selects the Quantity column and uses .sum() to add all the quantities together. 
# This gives the total number of product units sold from completed and paid transactions. 
# Finally, display(total_units_sold) displays the calculated total units sold.
total_units_sold = store_table[
    store_table["NetRevenue"] > 0
]["Quantity"].sum()

display(total_units_sold)

# COMMAND ----------

# MAGIC %md
# MAGIC - Completion Rate

# COMMAND ----------

# This code calculates the percentage of records that have a status of "Completed" in store_table. The .eq("Completed") checks each value in the Status column and returns True when the status is "Completed" and False for other statuses. 
# The .mean() calculates the proportion of True values, and multiplying by 100 converts that proportion into a percentage. 
# Finally, .round(2) rounds the result to two decimal places, and display(completion_rate) displays the completion rate.
completion_rate = (
    store_table["Status"].eq("Completed").mean() * 100
).round(2)

display(completion_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC - Cancelled + Returned Rate

# COMMAND ----------

# This code calculates the percentage of records that were either Cancelled or Returned in store_table. The .isin(["Cancelled", "Returned"]) checks the Status column and identifies records where the status is either "Cancelled" or "Returned". 
# The .mean() calculates the proportion of records that meet either condition, while multiplying by 100 converts that proportion into a percentage. 
# Finally, .round(2) rounds the result to two decimal places, and display(cancelled_returned_rate) displays the calculated cancelled
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

# This code calculates the percentage of payment records that have a status of "Failed" in store_table. The .eq("Failed") checks each value in the PaymentStatus column and returns True when the payment failed and False for other payment statuses. 
# The .mean() calculates the proportion of failed payments, while multiplying by 100 converts that proportion into a percentage. 
# Finally, .round(2) rounds the result to two decimal places, and display(payment_failure_rate) displays the calculated payment failure rate.
payment_failure_rate = (
    store_table["PaymentStatus"].eq("Failed").mean()
    * 100
).round(2)

display(payment_failure_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC - Average Discount

# COMMAND ----------

# This code calculates the average discount given across all records in store_table. The .mean() function calculates the average value of the Discount column, which is stored as a decimal, such as 0.10 for a 10% discount. 
# Multiplying the result by 100 converts the decimal into a percentage, while .round(2) rounds the percentage to two decimal places. 
# Finally, display(average_discount) displays the average discount percentage.
average_discount = (
    store_table["Discount"].mean() * 100
).round(2)

display(average_discount)

# COMMAND ----------

# MAGIC %md
# MAGIC - Total Customers

# COMMAND ----------

# This code calculates the total number of unique customers in store_table. The .nunique() function counts each different CustomerID only once, even if the same customer appears in multiple orders or transactions. 
# This gives the total number of distinct customers represented in the final dataset. 
# Finally, display(total_customers) displays the calculated customer count.
total_customers = store_table["CustomerID"].nunique()

display(total_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC - Monthly Net Revenue Trend

# COMMAND ----------

# This code calculates the total Net Revenue for each month using only transactions where NetRevenue is greater than zero. It groups the transactions by the OrderDate month using .dt.to_period("M"), adds the NetRevenue values for each month with .sum(), and then uses .reset_index() to turn the grouped result back into a DataFrame. 
# The code then converts the monthly period back into a standard date format using .dt.to_timestamp(), making the dates easier to use for analysis and visualisation. 
# Finally, display(monthly_revenue) displays the monthly revenue table.
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

# This code calculates the total number of realised orders for each month by first filtering store_table to include only records where NetRevenue is greater than zero. It then groups the records by the month of OrderDate using .dt.to_period("M") and counts the unique OrderID values with .nunique(), ensuring that an order with multiple product lines is counted only once. 
# The .reset_index(name="Orders") converts the result into a DataFrame and names the calculated column Orders. 
# The code then converts the monthly period back to a standard date format using .dt.to_timestamp() and displays the resulting monthly order table.
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

# This code calculates the Average Order Value (AOV) for each month using only realised orders where NetRevenue is greater than 0. It first groups the data by month and OrderID, then adds the NetRevenue for each order so that orders containing multiple product lines are treated as one complete order. 
# It then groups those order totals by month and calculates the average value of the orders in each month. 
# The result is rounded to two decimal places, the monthly period is converted back into a timestamp for easier use in charts and analysis, and display(monthly_aov) shows the final monthly AOV table.
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
# MAGIC - Net Revenue By Category

# COMMAND ----------

# This code creates a category-level performance table using only realised sales where NetRevenue is greater than 0. It groups the data by Category and calculates three key measures for each category: total NetRevenue, total UnitsSold, and the number of unique Orders. 
# The categories are then sorted from the highest to the lowest NetRevenue, the index is reset to create a clean table, and display(category_analysis) shows the final results for comparing which product categories generate the most revenue, sell the most units, and receive the most orders.
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

# This code calculates the total number of units sold for each product category, using only realised sales where NetRevenue is greater than 0. 
# It groups the data by Category, sums the Quantity for each category, sorts the categories from the highest to the lowest number of units sold, and resets the index while naming the result UnitsSold. 
# Finally, display(category_units) shows the category-level units sold table, which can be used to identify which categories have the highest sales volume.
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
# MAGIC - Top 10 Products By Net Revenue

# COMMAND ----------

# This code identifies the top 10 products by Net Revenue, using only realised sales where NetRevenue is greater than 0. It groups the data by ProductName and calculates the total NetRevenue, total UnitsSold, and number of unique Orders for each product. 
# The products are then sorted from the highest to the lowest Net Revenue, the first 10 products are selected, the index is reset to create a clean table, and the numerical results are rounded to two decimal places. 
# Finally, display(top_products_revenue) shows the top-performing products for analysis.
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

# This code identifies the top 10 products by units sold, using only realised sales where NetRevenue is greater than 0. 
# It groups the data by ProductName, adds together the Quantity sold for each product, sorts the products from the highest to the lowest number of units sold, selects the first 10 products, and resets the index while naming the result UnitsSold. 
# Finally, display(top_products_by_units_sold) shows the 10 products with the highest sales volume.
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
# MAGIC - Net Revenue By City

# COMMAND ----------

# This code analyses sales performance by city, using only realised sales where NetRevenue is greater than 0. It groups the data by City and calculates the total NetRevenue, the number of unique Orders, and the average NetRevenue per transaction as AOV. 
# The cities are then sorted from the highest to the lowest Net Revenue, the index is reset to create a clean table, and the results are rounded to two decimal places. 
# Finally, display(city_analysis) shows the city-level performance table, which helps identify which cities generate the most revenue, have the most orders, and have higher average transaction values.
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
# MAGIC - Net Revenue By Customer Segments

# COMMAND ----------

# This code analyses sales performance by customer segment, using only realised sales where NetRevenue is greater than 0. It groups the data by CustomerSegment and calculates the total NetRevenue, the number of unique Orders, and the average NetRevenue as AOV for each segment. 
# The segments are then sorted from the highest to the lowest Net Revenue, the index is reset to create a clean table, and the results are rounded to two decimal places. 
# Finally, display(segment_analysis) shows the performance of each customer segment, helping identify which segments generate the most revenue, orders, and average transaction value.
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
# MAGIC - Net Revenue By Age Bucket

# COMMAND ----------

# This code analyses sales performance by customer age group, using only realised sales where NetRevenue is greater than 0. It groups the data by AgeBucket and calculates the total NetRevenue and the number of unique Orders for each age group. 
# The age groups are then sorted from the highest to the lowest Net Revenue, the index is reset to create a clean table, and the results are rounded to two decimal places. 
# Finally, display(age_analysis) shows which age groups generate the most revenue and orders.
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

# This code analyses orders and revenue by order status, including statuses such as completed, cancelled, or returned. It groups the entire store_table by Status and calculates the number of unique Orders and the total Revenue for each status. 
# The results are then sorted from the highest to the lowest number of orders, the index is reset to create a clean table, and display(status_analysis) shows the final status analysis. 
# This helps determine how orders are distributed across different statuses and how much revenue is associated with each status.
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

# This code analyses orders and revenue by payment status. It groups the entire store_table by PaymentStatus and calculates the number of unique Orders and the total Revenue associated with each payment status. 
# The results are sorted from the highest to the lowest number of orders, the index is reset to create a clean table, and display(payment_status_analysis) shows the final payment-status analysis. 
# This helps identify how many orders were paid, failed, or have other payment statuses and the revenue associated with each payment status.
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

# This code analyses payment performance by payment method. It groups the data by PaymentMethod and calculates the number of unique orders, total NetRevenue, and number of failed payments for each payment method. 
# The lambda expression checks the PaymentStatus values and counts how many are "Failed". It then calculates the FailureRate by dividing failed payments by the number of orders and multiplying by 100 to express the result as a percentage. 
# Finally, the results are rounded to two decimal places and display(payment_method_analysis) shows the payment method performance table.
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

# This code analyses sales performance at different discount levels, using only realised sales where NetRevenue is greater than 0. It groups the data by Discount and calculates the number of unique Orders, the average quantity of items purchased, the average revenue per order line, and the total revenue generated at each discount level. 
# The results are then reset into a clean table, sorted from the lowest to the highest discount, and rounded to two decimal places. Finally, display(discount_analysis) shows the results, helping determine whether higher discounts are associated with larger quantities purchased or different revenue levels.
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

# This code calculates the average quantity of items purchased at each discount level, using only realised sales where NetRevenue is greater than 0. 
# It groups the data by Discount, calculates the mean Quantity for each discount level, names the resulting column AverageQuantity, and sorts the discount levels from lowest to highest. 
# The results are rounded to two decimal places, and display(discount_quantity) shows the final table, helping analyse whether higher discounts are associated with customers purchasing more items.
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
# MAGIC - Net Revenue by Day Classification

# COMMAND ----------

# This code analyses sales performance by day classification, using only realised sales where NetRevenue is greater than 0. It groups the data by day_classification_x and calculates the number of unique Orders and total NetRevenue for each day classification. It then calculates AOV by dividing the total Net Revenue by the number of orders, giving the average value of an order for each day classification. 
# The AOV is rounded to two decimal places, and display(day_analysis) shows the final comparison of orders, revenue, and AOV.
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
# MAGIC - Net Revenue by Day-of-Week

# COMMAND ----------

# This code analyses sales performance for each day of the week, using only realised sales where NetRevenue is greater than 0. It groups the data by DayName_x and calculates the number of unique Orders and total NetRevenue for each day. 
# The .reindex() arranges the results in the correct Monday-to-Sunday order instead of sorting the days alphabetically, while .reset_index() creates a clean table structure. 
# Finally, display(day_name_analysis) shows the number of orders and revenue generated
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
# MAGIC - Net Revenue by Quater

# COMMAND ----------

# This code analyses sales performance by quarter, using only realised sales where NetRevenue is greater than 0. It groups the data by Quarter_x and calculates the total NetRevenue and the number of unique Orders for each quarter. 
# The index is then reset to create a clean table, and display(quarter_analysis) shows the quarterly results, making it easier to compare revenue and order activity across the different quarters.
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
# MAGIC - Net Revenue by Year

# COMMAND ----------

# This code analyses sales performance by year, using only realised sales where NetRevenue is greater than 0. 
# It groups the data by Year_x and calculates the total NetRevenue and the number of unique Orders for each year. 
# The index is then reset to create a clean table, and display(year_analysis) shows the yearly results, making it easier to compare revenue and order activity between years.
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
# MAGIC - NetRevenue Year Over Year 

# COMMAND ----------

# This code calculates the Year-over-Year (YoY) growth rate in Net Revenue for each year in year_analysis. The .pct_change() function compares each year's NetRevenue with the previous year's Net Revenue and calculates the percentage change, which is then multiplied by 100 to express it as a percentage. 
# The results are rounded to two decimal places and stored in a new column called YoY_Growth. 
# The first year will have a blank value because there is no previous year available for comparison. 
# Finally, display(year_analysis) shows the updated yearly analysis with the YoY growth percentage.
year_analysis["YoY_Growth"] = (
    year_analysis["NetRevenue"]
    .pct_change()
    * 100
).round(2)

display(year_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC - Net Revenue v Orders

# COMMAND ----------

# This code creates a monthly performance table that tracks Net Revenue, Orders, AOV, and their month-over-month growth. It first filters the data to include only realised sales where NetRevenue is greater than 0, then groups the data by month and calculates total NetRevenue and unique Orders. 
# It calculates AOV by dividing monthly Net Revenue by the number of orders, then uses .pct_change() to calculate the percentage growth from the previous month for Orders, AOV, and NetRevenue. 
# All growth measures are multiplied by 100 and rounded to two decimal places. 
# Finally, display(monthly_performance) shows the complete monthly performance table, which can be used to identify changes and trends in the shop's monthly performance.
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