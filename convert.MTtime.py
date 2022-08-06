import csv
import glob
import pandas as pd
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import calendar
import shutil
import os
import sys
from datetime import datetime, date

def pickup_timename_today(timestamp): 

   lasttime = timestamp.iloc[-1]
   month = pd.DatetimeIndex(timestamp).month
   year = pd.DatetimeIndex(timestamp).year
   day = pd.DatetimeIndex(timestamp).day
   elast = len(month)
   del lasttime

   today = pd.Timestamp.now()

   timeframe = str(year[elast-1])+'_'+str('{:02}'.format(month[elast-1]))+'_'+str('{:02}'.format(day[elast-1])) \
        +'_' + str(today.year%100) + '_'+ str('{:02}'.format(today.month))+'_'+str('{:02}'.format(today.day))
 
   return timeframe

def pickup_timename(timestamp): 

   lasttime = timestamp.iloc[-1]
   month = pd.DatetimeIndex(timestamp).month
   year = pd.DatetimeIndex(timestamp).year
   day = pd.DatetimeIndex(timestamp).day
   elast = len(month)
   del lasttime

   timeframe = str(year[elast-1])+'_'+str('{:02}'.format(month[elast-1]))+'_'+str('{:02}'.format(day[elast-1])) 
 
   return timeframe

def addheaders(filename1,filename2,head,inum):

   with open(filename1+".csv", "r") as infile:
     reader = list(csv.reader(infile))
     reader.insert(inum, head)

   with open(filename2+".csv", "w") as outfile:
     writer = csv.writer(outfile)
     for line in reader:
         writer.writerow(line)

def dayofY_toTimeStamp(df,deltt):

    newdate = pd.to_datetime(df.loc[:,'Year'].values*10000000+df.loc[:,'Day_of_Year'].values*10000+df.loc[:,'HrMin'], format='%Y%j%H%M') \
            + pd.Timedelta(hours=deltt)

    return newdate

#---------------------------------------- 

indir0 = 'output/'
outdir0 = 'output.v02/'

#indir0 = 'input/'
#outdir0 = 'output/'

tmpdir = 'tmp/'

allyears = pd.Series([elem.replace(indir0,'') for elem in pd.Series(glob.glob(indir0+"*"))])
print(allyears)

for it in range(0,len(allyears)):
 
  iyr = allyears[it]

  findir = indir0 + iyr + "/"
  alllocates = pd.Series([elem.replace(findir,'') for elem in pd.Series(glob.glob(findir+"*"))])
  print(alllocates)

  for ii in range(0,len(alllocates)):

        locatename = os.path.splitext(alllocates.iloc[ii])[0]
        print(locatename)

        #print(' ----- Read CSV file ------')
        df = pd.read_csv(findir + '/'+ locatename +'.csv'\
          , delimiter = ',', skiprows=[0,2,3], na_values = ['NAN','"NAN"'], header=0, low_memory=False)
        print(df)

        #--- add additional information of headers ----

        stid = df.SiteNum

        hfile = findir+locatename+'.csv'
        with open(hfile, "r") as infile:
            head = list(csv.reader(infile))[0:4]

        # --- create timestamp

        timestamp = dayofY_toTimeStamp(df,-6) 

        record = range(0,len(timestamp))
        #   print(record)

        # --- create a new data frame
        newdf = pd.DataFrame(index=timestamp)

        newdf.index.names = ["TIMESTAMP"]
        newdf['RECORD']=record
        newdf['SiteNum']=stid.values
   
        # --- Update hours, day of year, and year with new MT timestamps
        newdf['HrMin'] = pd.DatetimeIndex(timestamp).hour*100
        newdf['Year'] = pd.DatetimeIndex(timestamp).year
        newdf['Day_of_Year'] = pd.DatetimeIndex(timestamp).day_of_year

        tmp = df.loc[:,"Lat":"XMTPWR"]
        tmp["TIMESTAMP"] = timestamp
        tmp.set_index('TIMESTAMP', inplace=True)

        newdf = pd.concat([newdf,tmp],axis=1)

        #--- create a new file name

        timeframe = pickup_timename(timestamp)   

        outdir = outdir0+iyr+'/'
        sys.exit() 
        #----
        # save data
        newdf.to_csv(tmpdir+filename,index=True, na_rep='NAN')

        fla = tmpdir+os.path.splitext(filename)[0]
        addheaders(fla,fla+'.2',head[0],0)
        addheaders(fla+'.2',fla+'.3',head[2],2)
        addheaders(fla+'.3',outdir+newfile,head[3],3)

        del df
        del newdf
        del timeframe
        del timestamp
        del newfile
        del head
        del tmp
 
print('end')

