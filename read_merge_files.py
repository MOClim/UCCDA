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

def check_station_number(df,stid):

    print('station id= ',stid.iloc[0])
    bool_series = df[df['SiteNum']!=stid.iloc[0]]

    if not bool_series.empty:
     print('different station data available')
     print('bool_series')
     print(bool_series)
    else:
     print('Correct station number')

    del bool_series

def remove_duplicated_date(df,iyr,locname,tlabel):

    df.reset_index(tlabel,inplace=True)
    bool_series = df[tlabel].duplicated(keep='first')


    if bool_series.any():

      print('duplicated')

      print('bool_series')
      print(bool_series.loc[bool_series==True])

      newdf = df.drop_duplicates(subset=[tlabel],keep="first")
 
      fnlog = False  # recording log of duplicated data
      if fnlog==True:
        diffdir = 'diff/'
        tmp = df[bool_series]
        newfile = diffdir+'/log.'+locname+'.'+iyr
        print(newfile)
        tmp.to_csv(newfile,index=True, na_rep='NaN')

        newfile = diffdir+'/log.'+locname+'.original.'+iyr
        print(newfile)
        df.to_csv(newfile,index=True, na_rep='NaN')
        del tmp

        bool_seriesb = df[tlabel].duplicated(keep='first')
        tmp = df[bool_seriesb]
        tmp = tmp.set_index(tlabel)
        newfile = diffdir+'/log.'+locname+'.v1.'+iyr
        print(newfile)
        tmp.to_csv(newfile,index=True, na_rep='NaN')
        del tmp
        del bool_seriesb

        bool_seriesb = df[tlabel].duplicated(keep=False)
        tmp = df[bool_seriesb]
        newfile = diffdir+'/log.'+locname+'.v3.'+iyr
        print(newfile)
        tmp.to_csv(newfile,index=True, na_rep='NaN')
        del tmp
        del bool_seriesb

    else:

     print('non duplicated')
     newdf = df

    del bool_series

    return newdf

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

def dayofY_toTimeStamp(df):

    newdate = pd.to_datetime(df.loc[:,'Year'].values*10000000+df.loc[:,'Day_of_Year'].values*10000+df.loc[:,'HrMin'], format='%Y%j%H%M')

    return newdate

#---------------------------------------- 


indir0 = 'data/v0.original/'
outdir = 'data/v01.timestamp/'


tlabel = 'TIMESTAMP'

allyears = pd.Series([elem.replace(indir0,'') for elem in pd.Series(glob.glob(indir0+"2*"))])
print(allyears)

for it in range(0,len(allyears)):

  iyr = allyears.iloc[it]

  indir = indir0+iyr+'/'

  alllocs = pd.Series([elem.replace(indir,'') for elem in pd.Series(glob.glob(indir+"*"))])
#  print(alllocs)

  for j in range(0,len(alllocs)):
  
    locname = alllocs[j]

    findir = indir + locname + '/'
    allfiles = pd.Series([elem.replace(findir,'') for elem in pd.Series(glob.glob(findir+"*"))])
#    print(allfiles)

    print(' ----------------------------')

    for ii in range(0, len(allfiles)):

       filename = os.path.splitext(allfiles.iloc[ii])[0]
       print(findir)

       #print(' ----- Read CSV file ------')
       df = pd.read_csv(findir+filename+'.csv' \
          , delimiter = ',', skiprows=[0,2,3], na_values = ['NAN','"NAN"'],header=0)
       #--- add additional information of headers ----

       stid = df.SiteNum

       hfile = findir+filename+'.csv'
       with open(hfile, "r") as infile:
            head = list(csv.reader(infile))[0:4]

       # --- check if the station number is the same for all data
       check_station_number(df,stid)

       # --- create timestamp
       # convert 'the days of the year' and time to the date/time (YYYY-MM-DD HH:MM:SS)
       timestamp = dayofY_toTimeStamp(df)

       # --- create a new dataframe
       df2 = pd.DataFrame(index=timestamp)
       df2.index.names = ['TIMESTAMP']

       df2['RECORD']=df.loc[:,'RECORD'].values
       df2['SiteNum']=stid.values

       # Extract data from header name "Year" to "XMTPWR"
       tmp = df.loc[:,'HrMin':'XMTPWR']
       tmp[tlabel] = timestamp
       tmp.set_index(tlabel, inplace=True)

       # Concatenating two datasets along with the column (axis=1)
       df2 = pd.concat([df2,tmp],axis=1)
       del tmp
       del df

       if ii==0:
          dat = df2
       else:
          tmp = dat
          del dat
          dat = pd.concat((tmp,df2),axis=0)
 
       del df2

    # --- remove duplicated data
    newdf = remove_duplicated_date(dat,iyr,locname,tlabel)

    #----
    # save the data
    #----

    print('save')
    newdf.set_index(tlabel,inplace=True)
    print(newdf)
    save_csv_file(newdf,outdir,iyr,locname,head)

    del dat
    del newdf 
    del head

