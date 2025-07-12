#Ví dụ về sử dụng Spark SQL truy vấn trên data frame (file .csv)
from pyspark.sql import SparkSession

# Khởi tạo một SparkSession
spark = SparkSession.builder \
    .appName("DistributedQueryExample") \
    .getOrCreate()

# Đọc dữ liệu từ file CSV và tạo DataFrame
df = spark.read.csv("data/orders_spark.csv", header=True, inferSchema=True)

# Đăng ký DataFrame như một bảng tạm thời để sử dụng với Spark SQL
df.createOrReplaceTempView("orders")

# Thực hiện truy vấn SQL phân tán
result1 = spark.sql("SELECT * FROM orders where amount = (select max(amount) from orders)")
# SQL đơn giản: SELECT FROM WHERE

result2 = df.filter("amount > 120").select("id", "orderid", "amount")

result3 = df.filter("amount < 500").select("id", "orderid", "amount").orderBy(df['amount'].desc())

result4 = df.filter("amount>100").select("id", "orderid", "amount").groupby("id", "orderid").agg({'amount':"sum"})

result5 = result2.join(result3, on='id', how='inner')

# # Hiển thị kết quả
#result1.show(n=result1.count()) #show hết
result2.show(n=result2.count())
result3.show(n=result3.count())
#result4.show(n=result4.count())
result5.show(n=result5.count())

# Đóng SparkSession
spark.stop()