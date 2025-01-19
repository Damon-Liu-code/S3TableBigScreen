from flask import Flask, render_template
from data import SourceData
from pyspark.sql import SparkSession

app = Flask(__name__)

def create_spark_session(bucket_arn):
    spark = (SparkSession.builder
        .appName("S3TablesExample")
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4,"
                "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.4")
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl",
                "software.amazon.s3tables.iceberg.S3TablesCatalog")
        .config("spark.sql.catalog.s3tablesbucket.warehouse", bucket_arn)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    return spark

BUCKET_ARN = "arn:aws:s3tables:us-west-2:034362073038:bucket/my-test-s3-table-bucket-demo"

# 创建 Spark 会话
spark = create_spark_session(BUCKET_ARN)

# 创建 SourceData 实例，传入 Spark 会话
data = SourceData(spark)

@app.route('/')
def index():
    return render_template('index.html', form=data, title=data.title)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
