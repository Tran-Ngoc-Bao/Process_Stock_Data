from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import *
import requests
from bs4 import BeautifulSoup
from datetime import datetime

schema = StructType([
		StructField("url", StringType(), False),
		StructField("content", StringType(), False)
	])

run_time = "{:%d%m%Y}".format(datetime.now())

def extract_load(link, find_con, url_header, classification):
	t = requests.get(link).content
	soup = BeautifulSoup(t, "html.parser")
	l = soup.find_all('a')
	data = []

	for i in l:
		if str(i).find(find_con) != -1:
			url = url_header + i["href"]
			
			try:
				content = requests.get(url, timeout = 5).content
			except requests.exceptions.Timeout:
				try:
					content = requests.get(url, timeout = 5).content
				except requests.exceptions.Timeout:
					try:
						content = requests.get(url, timeout = 5).content
					except requests.exceptions.Timeout:
						continue

			tmp = tuple((url, content.decode("utf-8")))
			data.append(tmp)
			print(tmp)

if __name__ == "__main__":
	sc = SparkContext("spark://spark-iceberg:7077", "extract_load")
	spark = SparkSession(sc)

	extract_load("https://apps.apple.com/vn/genre/ios-tr%C3%B2-ch%C6%A1i/id6014?l=vi", "/vn/app/", "", "app_store")