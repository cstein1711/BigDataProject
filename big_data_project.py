from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType
import geopandas as gpd
import pandas as pd

def extract_data(spark,startyear=2000, endyear=2015):
    co2_data = spark.read.option("inferSchema", "true").option("header", "true").csv(
        "C:/Users/Stone/Desktop/Uni/BigData/Project/API_EN.ATM.CO2E.PC_DS2_en_csv_v2_1217665.csv")
    return co2_data.select("Country Name", str(startyear), str(endyear))

def remove_null(word):
    if "null" in word:
        word = "0"
    return word




startyear=2000
endyear=2019
    
sc = SparkContext("local", "BigDataProject")
sc.setLogLevel("ERROR")
spark = SparkSession(sc)


result = extract_data(spark,startyear=startyear,endyear=endyear)

result = result.withColumn(str(startyear), result[str(startyear)].cast(DoubleType()))
result = result.withColumn(str(endyear), result[str(endyear)].cast(DoubleType()))

result = result.na.fill(0)

result.show()
#print(result)

data = result.select("*").toPandas()   
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
#for item  in world:
    #print(item)
    
for item in data['Country Name']:
    world_data_list = world['name'].tolist()
    if item in world_data_list:
      pass
    else:
      print(item)
    

    #world
    
    #print(len(world_data_list))
    #print(len(data))
      
world = world[['name', 'geometry']]

world = world.sort_values(by=['name'])

world.replace('Russia', 'Russian Federation', inplace= True)
world.replace('Syria', 'Syrian Arab Republic', inplace= True)
world.replace('United States of America', 'United States', inplace= True)
world.replace('Russia', 'Russian Federation', inplace= True)
world.replace('Russia', 'Russian Federation', inplace= True)
world.replace('Russia', 'Russian Federation', inplace= True)

    
data.rename(columns = {"Country Name" : 'name'}, inplace = True)
     
combined = world.merge(data, on = 'name')

combined['Diff']=combined[str(startyear)]-combined[str(endyear)]

    

combined = combined[combined['name'] != 'Antarctica']
combined.plot(column = 'Diff', cmap ='hsv',  legend=True)

