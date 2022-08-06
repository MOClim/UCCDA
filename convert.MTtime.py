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

columname = ["SiteNum","Year","Day_of_Year","HrMin","Lat","Lon","VersionNum","TAIR1(1)","TAIR1(2)","TAIR1(3)","TAIR1(4)","TAIR1(5)","TAIR1(6)","TAIR1(7)","TAIR1(8)","TAIR1(9)","TAIR1(10)","TAIR1(11)","TAIR1(12)","TAIR2(1)","TAIR2(2)","TAIR2(3)","TAIR2(4)","TAIR2(5)","TAIR2(6)","TAIR2(7)","TAIR2(8)","TAIR2(9)","TAIR2(10)","TAIR2(11)","TAIR2(12)","TAIR3(1)","TAIR3(2)","TAIR3(3)","TAIR3(4)","TAIR3(5)","TAIR3(6)","TAIR3(7)","TAIR3(8)","TAIR3(9)","TAIR3(10)","TAIR3(11)","TAIR3(12)","VWPCP1(1)","VWPCP1(2)","VWPCP1(3)","VWPCP1(4)","VWPCP1(5)","VWPCP1(6)","VWPCP1(7)","VWPCP1(8)","VWPCP1(9)","VWPCP1(10)","VWPCP1(11)","VWPCP1(12)","VWPCP2(1)","VWPCP2(2)","VWPCP2(3)","VWPCP2(4)","VWPCP2(5)","VWPCP2(6)","VWPCP2(7)","VWPCP2(8)","VWPCP2(9)","VWPCP2(10)","VWPCP2(11)","VWPCP2(12)","VWPCP3(1)","VWPCP3(2)","VWPCP3(3)","VWPCP3(4)","VWPCP3(5)","VWPCP3(6)","VWPCP3(7)","VWPCP3(8)","VWPCP3(9)","VWPCP3(10)","VWPCP3(11)","VWPCP3(12)","WET1(1)","WET1(2)","WET1(3)","WET1(4)","WET1(5)","WET1(6)","WET1(7)","WET1(8)","WET1(9)","WET1(10)","WET1(11)","WET1(12)","WET2(1)","WET2(2)","WET2(3)","WET2(4)","WET2(5)","WET2(6)","WET2(7)","WET2(8)","WET2(9)","WET2(10)","WET2(11)","WET2(12)","TAIR1_5mn_Min","TAIR2_5mn_Min","TAIR3_5mn_Min","TAIR1_5mn_Max","TAIR2_5mn_Max","TAIR3_5mn_Max","T1Cal_Std","T2Cal_Std","T3Cal_Std","FANSP1_Avg","FANSP2_Avg","BATVolt_Avg","PSVoltFL","DoorTime_Tot","TPRECP_Max","RefRes_Avg","DSignTR","TotHTR","TotDoor","XMTPWF","XMTPWR","unknown"]

indir0 = 'data/'
outdir0 = 'output/'

tmpdir = 'tmp/'

allnames = pd.Series([elem.replace(indir0,'') for elem in pd.Series(glob.glob(indir0+"*"))])

for iloc in range(0,len(allnames)):

    locname = allnames.iloc[iloc]

    indir = indir0+locname+'/'
    lst = pd.Series([elem.replace(indir,'') for elem in pd.Series(glob.glob(indir+"*"))])

    for filename in lst:

        #print(' ----- Read CSV file ------')
        df = pd.read_csv(indir+filename \
          , delimiter = ',', na_values = ['NAN','"NAN"'], header=None, low_memory=False)

        #--- add additional information of headers ----

        df.columns = columname
        stid = df.SiteNum

        hfile = '../headerlist/'+str(stid.iloc[0])+'.csv'
        with open(hfile, "r") as infile:
             head = list(csv.reader(infile))

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

        outdir = outdir0+'reformat/'+locname + '/'
        newfile = 'USCRN_Hr_TBL_'+ timeframe

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

