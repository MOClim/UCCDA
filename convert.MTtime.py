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

def save_csv_file(dat,outdir,iyr,filename,head):

    tmpdir = 'tmp/'
    filelist = glob.glob(os.path.join(tmpdir, "*"))
    for f in filelist:
        os.remove(f)

    dat.to_csv(tmpdir+filename+'.csv',index=True, na_rep='NAN')

    # add header information to the datasets
    fla = tmpdir+os.path.splitext(filename)[0]
    addheaders(fla,fla+'.2','.csv',head[0],0)
    addheaders(fla+'.2',fla+'.3','.csv',head[2],2)

    # save the dataset in the 'outdir' directory
    newfile = outdir+iyr+'/'+filename
    filen = newfile + '.csv'
    os.system('rm '+filen)

    addheaders(fla+'.3',newfile,'.csv',head[3],3)
    print(newfile+'.csv')

def addheaders(filename1,filename2,ext,head,inum):

   with open(filename1+ext, "r") as infile:
     reader = list(csv.reader(infile))
     reader.insert(inum, head)

   with open(filename2+ext, "w") as outfile:
     writer = csv.writer(outfile)
     for line in reader:
         writer.writerow(line)

def dayofY_toTimeStamp(df,deltt):

    newdate = pd.to_datetime(df.loc[:,'Year'].values*10000000+df.loc[:,'Day_of_Year'].values*10000+df.loc[:,'HrMin'], format='%Y%j%H%M') \
            + pd.Timedelta(hours=deltt)

    return newdate

#---------------------------------------- 

indir0 = 'output/'
outdir = 'output.v02/'

#indir0 = 'input/'
#outdir = 'output/'

tmpdir = 'tmp/'

allyears = pd.Series([elem.replace(indir0,'') for elem in pd.Series(glob.glob(indir0+"*"))])
print(allyears)

#for it in range(0,len(allyears)):
for it in range(0,1):
 
  iyr = allyears[it]

  findir = indir0 + iyr + "/"
  alllocates = pd.Series([elem.replace(findir,'') for elem in pd.Series(glob.glob(findir+"*"))])
  print(alllocates)

#  for ii in range(0,len(alllocates)):
  for ii in range(0,1):

        locname = os.path.splitext(alllocates.iloc[ii])[0]
        print(locname)

        #print(' ----- Read CSV file ------')
        df = pd.read_csv(findir + '/'+ locname +'.csv'\
          , delimiter = ',', skiprows=[0,2,3], na_values = ['NAN','"NAN"'], header=0, low_memory=False)
        print(df)

        #--- add additional information of headers ----

        stid = df.SiteNum

        hfile = findir+locname+'.csv'
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

        #----
        # save data

        print('save')
        print(newdf)
        save_csv_file(newdf,outdir,iyr,locname,head)


        del df
        del newdf
        del timestamp
        del head
        del tmp
 

