import pyodbc
import pandas as pd
import io
import censusgeocode as cg
import numpy as np
import time
import sys
import threading
import math
import os

start_time = time.time()
server = os.environ['SQL_URL']
database = os.environ['SQL_DB']
username = os.environ['SQL_USR']
password = os.environ['SQL_PWD']  
drivers = [item for item in pyodbc.drivers()]
driver = drivers[-1]
print("driver:{}".format(driver))

output = io.StringIO()
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
record_count_sql = '''SELECT count(ca.recordId)

FROM staging.clientAddress ca

LEFT JOIN lookup.geocodeInfo gi

ON ca.recordId = gi.recordId  and ca.id = gi.addrId

WHERE gi.addrId IS NULL;'''
cursor.execute(record_count_sql)
row = cursor.fetchone()
print("Total rows detected for geocoding is "+str(row[0]))
rowcount = int(row[0])
runs_possible = math.ceil(rowcount/1000)
print(str(runs_possible)+" before adjustment")
if runs_possible > 10:
    print("Setting maximum runs to API limit of 10,000")
    runs_possible = 10
elif runs_possible == 0:
    print("There are no lines to geocode")
    quit()
else: runs_possible = runs_possible
print(str(runs_possible)+" runs of 1000 in scope for this function")


sql = '''SELECT TOP {} concat(ca.recordId, ca.id) as uniqueid, ca.address1, ca.city, ca.state, ca.zipcode

FROM staging.clientAddress ca

LEFT JOIN lookup.geocodeInfo gi

ON ca.recordId = gi.recordId  and ca.id = gi.addrId

WHERE gi.addrId IS NULL'''.format(runs_possible*1000)

df = pd.read_sql(sql,cnxn)
frames = {"Command":["df1000 = df.iloc[:1000,:]","df2000 = df.iloc[1000:2000]","df3000 = df.iloc[2000:3000]","df4000 = df.iloc[3000:4000]","df5000 = df.iloc[4000:5000]","df6000 = df.iloc[5000:6000]","df7000 = df.iloc[6000:7000]","df8000 = df.iloc[7000:8000]","df9000 = df.iloc[8000:9000]", "df10000 = df.iloc[9000:10000]"]}
for i in range(0, runs_possible):
    exec(frames["Command"][i]) 
catch = []
def geocode(threadName, operator):
    print(threadName+" "+time.asctime())
    start_time = time.time()
    output = io.StringIO()
    catch = []
    operator.to_csv(output, index=False, header=False)
    catch = cg.addressbatch(output, returntype='geographies')
    operator = pd.DataFrame(catch)
    operator["recordid"] = operator.id.str[:36]
    operator["addrid"] = operator.id.str[36:]
    operator.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
    operator = operator.fillna(0)
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    for index, row in operator.iterrows():
        cursor.execute(
            '''INSERT INTO [lookup].[geocodeInfo]
            ([uniqueId]
            ,[address]
            ,[match]
            ,[matchType]
            ,[parsed]
            ,[tigerlineId]
            ,[side]
            ,[statefp]
            ,[countyfp]
            ,[tract]
            ,[block]
            ,[lat]
            ,[lon]
            ,[recordId]
            ,[addrId]
            ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                row.id, row.address, row.match, row.matchtype, row.parsed, row.tigerlineid, row.side, row.statefp, row.countyfp, row.tract, row.block, row.lat, row.lon, row.recordid, row.addrid)
    cnxn.commit()
    cursor.close()
    print(threadName+" has completed successfully in --- %s seconds ---" % (time.time() - start_time))
    time.sleep(5)
    sys.stdout.flush()

try:
    t1 = threading.Thread(target=geocode, args=("Geocode 0-1000", df1000,))
    t2 = threading.Thread(target=geocode, args=("Geocode 1000-2000", df2000,))
    t3 = threading.Thread(target=geocode, args=("Geocode 2000-3000", df3000,))
    t4 = threading.Thread(target=geocode, args=("Geocode 3000-4000", df4000,))
    t5 = threading.Thread(target=geocode, args=("Geocode 4000-5000", df5000,))
    t6 = threading.Thread(target=geocode, args=("Geocode 5000-6000", df6000,))
    t7 = threading.Thread(target=geocode, args=("Geocode 6000-7000", df7000,))
    t8 = threading.Thread(target=geocode, args=("Geocode 7000-8000", df8000,))
    t9 = threading.Thread(target=geocode, args=("Geocode 8000-9000", df9000,))
    t10 = threading.Thread(target=geocode, args=("Geocode 9000-10000", df10000,))
    commands = {"Command":["t1.start()", "t2.start()", "t3.start()", "t4.start()", "t5.start()", "t6.start()", "t7.start()", "t8.start()", "t9.start()", "t10.start()"]}
    for i in range(0, runs_possible):
        eval(commands["Command"][i])
except:
    print("something has gone wrong")
print("All threads have been generated in a total of %s seconds" % (time.time() - start_time))
