//imports
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LogProcessor {

    def main(args:Array[String]){

        //create session
        val spark = SparkSession
          .builder
          .appName("LogProcessor")
          .getOrCreate()

        import spark.implicits._

        val dfRaw = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe","LogData")
          .option("startingOffsets","earliest")
          .load()
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
		  
		
		val sourceDF = dfRaw.filter($"value".contains("\"event\":\"pageView\""))
		
		val sucessDF = sourceDF.filter($"value".contains("\"page_type\":\"success\"") || $"value".contains("\"page_type\":\"cart\""))

		val successSchema = StructType(Seq(
			StructField("user_id",StringType,true),
			StructField("session_id",StringType,true),
			StructField("partner_id",StringType,true),
			StructField("partner_name",StringType,true),
			StructField("country",StringType,true),
			StructField("city",StringType,true),
			StructField("date",StringType,true),
			StructField("cart_amount",StringType,true),
			StructField("page_type",StringType,true),
			StructField("products",ArrayType(StructType(Seq(
			  StructField("category",ArrayType(StringType,true),true),
			  StructField("name",StringType,true),
			  StructField("price",DoubleType,true),
			  StructField("id",StringType,true))
			  ),true),true)
			))

		val sucessParsedDF = sucessDF.select(from_json($"value",successSchema).as("data")).select("data.*")

		val successExplodedDF = sucessParsedDF.withColumn("product",explode($"products")).drop("products")

		val successExtractedDF = successExplodedDF.select("user_id","session_id","partner_id","partner_name","country","city","date","cart_amount","page_type","product.*").drop("product")

		val successFinalDF = successExtractedDF.select("user_id","session_id","partner_id","partner_name","country","city","date","cart_amount","page_type","id", "price", "name", "category")

		val productDF = sourceDF.filter($"value".contains("\"page_type\":\"productDetail\""))

		val productSchema = StructType(Seq(
			StructField("user_id",StringType,true),
			StructField("session_id",StringType,true),
			StructField("partner_id",StringType,true),
			StructField("partner_name",StringType,true),
			StructField("country",StringType,true),
			StructField("city",StringType,true),
			StructField("date",StringType,true),
			StructField("cart_amount",DoubleType,true),
			StructField("page_type",StringType,true),
			StructField("product",StructType(Seq(
			  StructField("category",ArrayType(StringType,true),true),
			  StructField("name",StringType,true),
			  StructField("id",StringType,true),
			  StructField("price",StructType(Seq(
				StructField("value",DoubleType,true),
				StructField("currency",StringType,true))),true)
			  )),true)
			))

		val productParsedDF = productDF.select(from_json($"value",productSchema).as("data")).select("data.*")

		val productExtractedDF = productParsedDF.select("user_id","session_id","partner_id","partner_name","country","city","date","cart_amount","page_type","product.*").drop("product")

		val productEnrichedDF = productExtractedDF.withColumn("price",$"price.value")

		val productFinalDF = productEnrichedDF.select("user_id","session_id","partner_id","partner_name","country","city","date","cart_amount","page_type","id", "price", "name", "category")


		val finalDF = successFinalDF.union(productFinalDF)
		
		finalDF
		  .writeStream
		  .format("json")
		  .option("checkpointLocation", "file:///G:/ms_data_engineer/checkpoint")
		  .option("path", "file:///G:/ms_data_engineer/output")
		  .partitionBy("date")
		  .start()
		  .awaitTermination()
	
    }

}
