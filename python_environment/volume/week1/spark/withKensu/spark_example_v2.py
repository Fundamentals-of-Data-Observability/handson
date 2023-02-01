
from kensu.pyspark import init_kensu_spark

kensu_agent_jar_path = 'kensu-spark-collector-1.0.0_spark-3.0.1.jar'

month = 'feb'
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').config("spark.driver.extraClassPath", kensu_agent_jar_path).getOrCreate()


init_kensu_spark(spark, process_name='Spark', code_version='v2')


dataset_url=f"../../data/green_ingestion.csv"

df = spark.read.format('csv').option('inferSchema','true').option('header','true').load(dataset_url)


df.createOrReplaceTempView('all_rides')

#passenger_count,trip_distance,total_amount

KPI = spark.sql('''SELECT VendorID, 
                sum(total_amount) AS total_revenue, 
                avg(total_amount) AS average_revenue,
                sum(passenger_count) AS total_passengers,
                avg(trip_distance) AS  average_distance,
                sum(trip_distance) AS total_miles,
                sum(trip_distance)*0.3 AS company_bill,
                count(*) AS number_of_rides
                FROM all_rides
                WHERE (trip_distance IS NOT NULL) AND (VendorID = '3')
                GROUP BY VendorID
                ''')

KPI.show()

dst_uri = f"../../data/{month}/green_ingestion_KPI"
KPI.write.mode('overwrite').save(dst_uri)


kpi = spark.read.option('inferSchema','true').load(dst_uri)
company_bill = kpi.select('VendorID','company_bill')
company_bill_uri = f"../../data/{month}/company_bill"
company_bill.write.mode('overwrite').save(company_bill_uri)

spark.stop()