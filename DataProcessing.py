# -*- coding: utf-8 -*-
import glob
import pandas as pd
import sqlite3
import shutil

#ASSUMPTIONS:
# The following assumptions were made for the DataProcessing.py
# 1. The sales files will always start with sales
# 2. Exclude sales dates that are not between the regions start and end date
# 3. No sales and regional data will be published when the programming is running
# 4. The job will be scheduled via crontab to run 

#PREREQUISITES FOR SOLUTION INSTALLTION
# 1. The solution will run on the linux server
# 2. Python needs to be installed on the linux server
# 3. All python libraries need to be download

#SCALING NOTES
# 1. The limitations in scaling the current solution are that
#     we have to additional monitoring jobs for storage space and deleting archived files, 
#     an increase in sales data volume will lead to an increase on processing time
# 2.  In order to scale the solution we will need to move away from capturing information in csv file
#     A web front end can be created to capture the information, which directly  store information in a database
#     A reporting solution can be developed to return the necessary summary data

#The folder for the Sales csv files
sales_files = glob.glob("Data/Sales/sales*.csv")
region_files = glob.glob("Data/Region/region*.csv")
arc_sales_files = "Data\Archive"

#Get all the Sales files and concatenate
try:
    df_sales = pd.concat((pd.read_csv(f) for f in sales_files),ignore_index=True)
except:
    print("Issue with Sales data files")
    exit
    
#Get the regional data
try:
    df_region = pd.concat((pd.read_csv(f) for f in region_files),ignore_index=True)
except:
    print("Issue with Regional data files")
    exit
    
#convert the columns to date
df_sales['Date'] = pd.to_datetime(df_sales['Date'])
df_region['StartDate'] = pd.to_datetime(df_region['StartDate'])
df_region['EndDate'] = pd.to_datetime(df_region['EndDate'])

#Make the db in memory
conn = sqlite3.connect(':memory:')

#create the SQL tables
df_sales.to_sql('sales', conn, index=False)
df_region.to_sql('region', conn, index=False)



#Add region description to sales data
#Ensure the sales date is between the region start and end date
#Summary of sales per region
query = '''
    select  
       sum(s.amount) as total_sales,count(s.network) as number_sales,s.network,r.regiondescription
    from
        sales s , 
        region r 
    where
            s.region =r.region and
            s.date between r.startdate and coalesce(r.enddate,date()) 
    group by s.network,r.regiondescription
    '''
df = pd.read_sql_query(query, conn)

#Write to sales regional summary output folder
df.to_csv(r'Data/Output/SalesRegionSummary.csv')

#move the sales files to the archive location
for f in sales_files:
    try:
        shutil.move(f, arc_sales_files)
    except:
        print ("Issue moving sales files to archive: "+f)
