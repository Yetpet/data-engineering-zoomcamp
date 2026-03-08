####Testing Spark using scala on spark-shell
scala> val data = 1 to 10000
scala> val distData = sc.parallelize(data)
scala> distData.filter(_ < 10).collect()


StructType(
    types.types.StructField('VendorID',  IntegerType(), True),
    types.StructField('tpep_pickup_datetime', TimestampNTZType(), True),
    types.StructField('tpep_dropoff_datetime', TimestampNTZType(), True),
    types.StructField('passenger_count', LongType(), True),
    types.StructField('trip_distance', DoubleType(), True),
    types.StructField('RatecodeID', LongType(), True),
    types.StructField('store_and_fwd_flag', StringType(), True),
    types.StructField('PULocationID', IntegerType(), True),
    types.StructField('DOLocationID', IntegerType(), True),
    types.StructField('payment_type', LongType(), True),
    types.StructField('fare_amount', DoubleType(), True),
    types.StructField('extra', DoubleType(), True),
    types.StructField('mta_tax', DoubleType(), True),
    types.StructField('tip_amount', DoubleType(), True),
    types.StructField('tolls_amount', DoubleType(), True),
    types.StructField('improvement_surcharge', DoubleType(), True),
    types.StructField('total_amount', DoubleType(), True),
    types.StructField('congestion_surcharge', DoubleType(), True),
    types.StructField('Airport_fee', DoubleType(), True),
    types.StructField('cbd_congestion_fee', DoubleType(), True)
    
)