# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
		explode, split, col, lower, trim,udf,  count, when, coalesce, lit, monotonically_increasing_id, concat
)
from pyspark.sql.types import BooleanType, ArrayType, StringType

spark = SparkSession.builder.appName("ReviewKeywordCount").getOrCreate()
review_df = spark.read.csv(
		"hdfs:///user/maria_dev/pre_data/test_pre_review/*.csv", header=True
)

keyword_df = spark.read.csv(
		"hdfs:///user/maria_dev/pre_data/gangnamKeyword-modify.csv", header=True
)

keywords = keyword_df.select('keyword').rdd.flatMap(lambda x: x).collect()
keywords = [kw.strip().lower() for kw in keywords]
broadcast_keywords = spark.sparkContext.broadcast(keywords)

review_df = review_df.withColumn('id', monotonically_increasing_id())
review_df = review_df.withColumn('title_words', split(col('title_pos'), ' '))
review_df = review_df.withColumn('content_words', split(col('content_pos'), ' '))

def concat_arrays(a,b):
	return (a or []) + (b or [])
concat_array_udf = udf(concat_arrays, ArrayType(StringType()))

review_df = review_df.withColumn('all_words', concat_array_udf(col('title_words'),col('content_words')))

review_words = review_df.select(
		 'id', 'title_pos', 'content_pos', 'hash_tag', explode('all_words').alias('word')
)

review_words = review_words.withColumn('word', lower(trim(col('word'))))

def is_ad_keyword(word):
	return word in broadcast_keywords.value

is_ad_keyword_udf = udf(is_ad_keyword, BooleanType())

review_words = review_words.withColumn('is_ad_keyword', is_ad_keyword_udf('word'))

ad_word_counts = review_words.filter(col('is_ad_keyword')).groupBy('id').agg(
		count('word').alias('ad_word_count')
)

review_with_counts = review_df.join(ad_word_counts, on='id', how='left')

review_with_counts = review_with_counts.withColumn(
		'ad_word_count', coalesce(col('ad_word_count'), lit(0))
)
review_with_counts = review_with_counts.withColumn(
		 'is_ad_review', when(col('ad_word_count') > 0, True).otherwise(False)
)

result_df = review_with_counts.select(
		 'id', 'title_pos', 'ad_word_count', 'is_ad_review'
)

result_df.write.csv(
		'hdfs:///user/maria_dev/test-result/th0/review_ad_word_counts', header=True, mode='overwrite'
)

spark.stop()
