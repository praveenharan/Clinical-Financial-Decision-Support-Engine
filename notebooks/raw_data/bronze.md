%pip install faker

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, expr, date_format, year, month, dayofmonth, quarter, dayofweek, explode, floor, rand, round, when
import random
from faker import *
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
