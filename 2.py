
# coding: utf-8

# # Live Server

# In[3]:


#find meeting info
with open('meetingTime.txt', 'r') as myfile:
    meetingTime=myfile.read().replace('\n', '')

with open('meetingLink.txt', 'r') as myfile:
    meetingLink=myfile.read().replace('\n', '')
meetingID=meetingLink.rsplit('/', 1)[1]
video_name= 'gomeeting' + meetingID + ".mp4"


# In[58]:


#upload meeting Details in container
import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess
block_blob_service = BlockBlobService(account_name='intelligentappstorage01', account_key='******************************************b/66CBJTd/Xg8j1ITmmGg==') 
container_name = "meetingdetails-edgevm"
block_blob_service.create_container(container_name)    #(for creating new container)
# Set the permission so the blobs are public.
block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
local_path=os.path.expanduser(r"C:\demo")
full_path_to_file =os.path.join(local_path, 'meetingTime.txt')
block_blob_service.create_blob_from_path(container_name, 'meetingTime.txt', full_path_to_file)
full_path_to_file =os.path.join(local_path, 'meetingLink.txt')
block_blob_service.create_blob_from_path(container_name, 'meetingLink.txt', full_path_to_file)

#open rdp
ip = "40.76.40.34"
l1="cmdkey /add:" + ip + " " +"/user:edgevm /pass:EVOKE@123pqrs" 
l2="mstsc /v:"+ ip
f = open('RDP.bat','w')
f.write(str(l1))
f.write("\n")
f.write(str(l2))
f.close()
import subprocess
subprocess.Popen('RDP.bat')

#Reading msg from service Bus
while True:
    from azure.servicebus import ServiceBusService, Message, Queue
    bus_service = ServiceBusService(
        service_namespace='SVDServiceBus',
        shared_access_key_name='RootManageSharedAccessKey',
        shared_access_key_value='**********************UXm0hGnxezXyx0=')
    msg = bus_service.receive_queue_message('resultqueue', peek_lock=True)
    print(msg.body)
    if(msg.body ==bytes(video_name, encoding='utf-8')):
        break
msg.delete()

# In[2]:


with open('meetingLink.txt', 'r') as myfile:
    meetingLink=myfile.read().replace('\n', '')
meetingID=meetingLink.rsplit('/', 1)[1]

from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
account_name='intelligentappstorage01'
account_key='WY306&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&b/66CBJTd/Xg8j1ITmmGg=='
block_blob_service = BlockBlobService(account_name, account_key)

generator = block_blob_service.list_blobs('meetingrecordings')

local_path =r"C:\demo\gomeeting" + meetingID + ".mp4"
block_blob_service.get_blob_to_path('meetingrecordings',"gomeeting"+meetingID+".mp4", local_path)


# In[ ]:


#Downlaod file from container

with open('meetingLink.txt', 'r') as myfile:
    meetingLink=myfile.read().replace('\n', '')
meetingID=meetingLink.rsplit('/', 1)[1]

from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
account_name='intelligentappstorage01'
account_key='WY3062K+hMvWGuZG2W******************************BJTd/Xg8j1ITmmGg=='
block_blob_service = BlockBlobService(account_name, account_key)

generator = block_blob_service.list_blobs('meetingrecordings')

local_path =r"C:\demo\gomeeting" + meetingID + ".mp4"
block_blob_service.get_blob_to_path('meetingrecordings',"gomeeting"+meetingID+".mp4", local_path)



#*****create frames******
import os
try:
    if not os.path.exists('gomeetingframes'+ meetingID):
        os.makedirs('gomeetingframes'+ meetingID)
except OSError:
    print ('Error: Creating directory of data')
import subprocess
subprocess.call(['ffmpeg', '-i', "gomeeting" + meetingID + ".mp4",  '-vf', 'fps=1', 'gomeetingframes'+meetingID+'/out%d.png'])


#*******crop images****
from PIL import Image
import os.path, sys
import os
import numpy as np
from natsort import natsorted, ns         #library for natural sorting of files
path = "C:/demo/gomeetingframes"+meetingID
images = os.listdir(path)
try:
    if not os.path.exists('crop_gomeetingframes'+meetingID):
        os.makedirs('crop_gomeetingframes'+meetingID)
except OSError:
    print ('Error: Creating directory of data')
sortimages=natsorted(images, alg=ns.IGNORECASE)
for i in range(len(sortimages)):
    im = Image.open("C:/demo/gomeetingframes" + meetingID +"/"+ sortimages[i])
    im = im.crop((2185,77,2554,224)) #Image map can be get from https://www.image-map.net/1519,80,1917,212
    name = './crop_gomeetingframes'+ meetingID +'/cropframe' +str(i) + '.png'
    im.save(name, "JPEG", quality=100)

    


# In[ ]:


#***************************OCR*****************************************************
#pip install opencv-python
import time 
import requests
import cv2
import operator
import numpy as np
import json
# Import library to display results
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
get_ipython().run_line_magic('matplotlib', 'inline')

# Variables
rest = []
_url = 'https://eastus.api.cognitive.microsoft.com/vision/v1.0/ocr'
_key = 'ea8b5ca45bdd44f2a7938fffc82bb110'
_maxNumRetries = 100

def processRequest( json, data, headers, params ):

    retries = 0
    result = None

    while True:
        response = requests.request( 'post', _url, json = json, data = data, headers = headers, params = params )

        if response.status_code == 429:
            print( "Message: %s" % ( response.json() ) )
            if retries <= _maxNumRetries: 
                time.sleep(1) 
                retries += 1
                continue
            else: 
                print( 'Error: failed after retrying!' )
                break
        elif response.status_code == 202:
            result = response.headers['Operation-Location']
        else:
            print( "Response code: %d" % ( response.status_code ) )
            res = response.text
            rest.append(eval(res))
            #res = json.loads(res)
            print( "Message: %s" % ( res ) )
        break
        
    return result

def getOCRTextResult( operationLocation, headers ):

    retries = 0
    result = None

    while True:
        response = requests.request('get', operationLocation, json=None, data=None, headers=headers, params=None)
        if response.status_code == 429:
            print("Message: %s" % (response.json()))
            if retries <= _maxNumRetries:
                time.sleep(1)
                retries += 1
                continue
            else:
                print('Error: failed after retrying!')
                break
        elif response.status_code == 200:
            result = response.json()
            res = response.text
            rest.append(eval(res))
        else:
            print("Error code: %d" % (response.status_code))
            res = response.text
            rest.append(eval(res))
            print("Message: %s" % (res))
        break

    return result


# Load raw image file into memory
pathToFileInDisk = "C:/demo/crop_gomeetingframes"+meetingID + "\\"
import glob
from natsort import natsorted, ns  
a = glob.glob(pathToFileInDisk + "*.png")
b= sortimages=natsorted(a, alg=ns.IGNORECASE)

rest1 = []
from PIL import Image
for i in range(len(a)):
    
    try:
        
        with open(b[i], 'rb') as f:
            jpgdata = f.read()
            
            # Computer Vision parameters
            params = {'language': 'en', 'detectOrientation': 'true'}

            headers = dict()
            headers['Ocp-Apim-Subscription-Key'] = _key
            headers['Content-Type'] = 'application/octet-stream'

            json = None

            operationLocation = processRequest(json, jpgdata, headers, params)

            result = None
            if (operationLocation != None):
                headers = {}
                headers['Ocp-Apim-Subscription-Key'] = _key
                while True:
                    time.sleep(1)
                    result = getOCRTextResult(operationLocation, headers)
                    c.append(result)
                    if result['status'] == 'Succeeded' or result['status'] == 'Failed':
                        break
            print(b[i])
    except Exception as e:
        print('does not have: %s',e)
        
###############################################################################################################################
###############################################################################################################################

import pandas as pd
import numpy as np
JS = []
for i in range(len(rest)):
    
    a1 = rest[i]
    a2 = a1['regions']
    for j  in range(len(a2)):
        a3 = a2[j]['lines']
        for k in range(len(a3)):
            a4 =a3[k]['words']
            for l in range(len(a4)):
                a5 = a4[l]['text']
                JS.append(a4)
                print()                
dfr=pd.DataFrame(JS)
for j in range(len(dfr.columns)): 
    for i in range(len(dfr[j])):
        if (dfr[j][i] == None):
            dfr[j][i]=None
        else:
            dfr[j][i]=dfr[j][i]['text']           
dfr1 = dfr.replace(np.nan, '', regex=True)

if len(dfr1.columns)==1:
    dfrnew=dfr1[0]  
else: 
    if len(dfr1.columns)==2:
        dfrnew=dfr1[0]+" "+dfr1[1]
    else:
        if len(dfr1.columns)==3:
            dfrnew=dfr1[0]+" "+dfr1[1]+" "+dfr1[2]
        else:
            if len(dfr1.columns)==4:
                dfrnew=dfr1[0]+" "+dfr1[1]+" "+dfr1[2]+" "+dfr1[3]
            else:
                if len(dfr1.columns)==5:
                    dfrnew=dfr1[0]+" "+dfr1[1]+" "+dfr1[2]+" "+dfr1[3]+" "+dfr1[4]

                    
                    
#
###
#####
#######
labels = (dfrnew != dfrnew.shift()).cumsum()
df4 = pd.concat([dfrnew,labels],axis = 1,names = 'label')
df4.columns = ['Speaker','label']
df5 = df4.reset_index().groupby(['label','Speaker']).agg(['min', 'max']).rename(
    columns={'min': 'Starttime', 'max': 'Endtime'}).reset_index()
df5.columns = [' '.join(col) for col in df5.columns]
df5.rename(columns={'index Starttime': 'Starttime', 'index Endtime': 'Endtime', 'Speaker ':'Speaker', 'label ':'label'}, inplace=True)
df=df5.drop(['label'],axis=1)
df["ChunkNo"]=range(len(df))
df["RowKey"]=""



# In[ ]:


#Convert Video file in Audio(.wav)  
import glob
import os
import threading
import time
import subprocess
videoname= "gomeeting" + meetingID + ".mp4"
audioname ="gomeeting" + meetingID + ".wav"
subprocess.call(('ffmpeg -i "%s" "%s"')%(videoname, audioname),shell=True)

#****access azure storage & create blob and table****
import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess
block_blob_service = BlockBlobService(account_name='intelligentappstorage01', account_key='WY3****************************************'
container_name = "audiofiles"
block_blob_service.create_container(container_name)    #(for creating new container)
# Set the permission so the blobs are public.
block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)

#***convert video in audio chunks as df & upload in container*****
#************[Azure transcribe function will not work for .mp3 file it will work for .wav audio files]
import os
import subprocess

with open('meetingLink.txt', 'r') as myfile:
    meetingLink=myfile.read().replace('\n', '')
meetingID=meetingLink.rsplit('/', 1)[1]
for i in range(len(df)):
    chunk=df['ChunkNo'][i]
    starttime=df['Starttime'][i]
    endtime=df['Endtime'][i]
    filename = meetingID +"_" + "chunk" + str(chunk) + "_" + str(starttime) + "_" + str(endtime) + ".wav"
    df['RowKey'][i] = filename     
    subprocess.call(['ffmpeg', '-i', str(audioname) , '-ss', str(starttime), '-t', str(endtime-starttime), '-acodec', 'copy', filename])   
    local_path=os.path.expanduser(r"C:/demo")
    local_file_name = meetingID +"_" + "chunk" + str(chunk) + "_" + str(starttime) + "_" + str(endtime) + ".wav"
    full_path_to_file =os.path.join(local_path, local_file_name)
    block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)
    time.sleep(2)
df.to_excel('result' + meetingID +'.xlsx')
import time
timeout = time.time() + 1800 
j=1
while True:
    # wait time in sec's
    time.sleep(90)
    #import table from storage as csv file

    import azure
    from azure.storage.table import TableService, Entity
    from azure.storage.table.models import EntityProperty
    import csv

    key ='******************************d/Xg8j1ITmmGg=='
    table_service = TableService(account_name='intelligentappstorage01', account_key=key)

    
    rows = table_service.query_entities('transcriptiontable') 
        
            

    with open('chunks'+meetingID+'.csv', 'w') as f:  
        w = None

        for a_row in rows:
            if w == None:
                w = csv.writer(f, lineterminator='\n')
                w.writerow(a_row.keys())
            row = []
            for key, value in a_row.items():
                actual_value = value
                if isinstance(value, EntityProperty):
                    actual_value = value.value
                row.append(actual_value)

            w.writerow(row)

    #Upate result in result file
    import pandas as pd
    import numpy as np
    from natsort import natsorted
    from natsort import natsorted, index_natsorted, order_by_index
    xl = pd.ExcelFile("result"+meetingID+".xlsx")
    df = xl.parse("Sheet1") 
    
    df2 = pd.read_csv('chunks'+meetingID+'.csv')
    final_df=pd.merge(df, df2, on='RowKey', how='outer')
    j = j + 1
    print("Result updation is still in progress!")
    os.remove('chunks'+meetingID+'.csv')
    if(final_df.isnull().any().any() == False) or time.time() > timeout:
        break
    
#**********************Now update final df into excel*******************         
final_df.to_excel('result' + meetingID +'.xlsx')

#delete container & full table/intermittent chunks from Azure table storage.
import azure
from azure.storage.table import TableService, Entity
from azure.storage.table.models import EntityProperty
from azure.storage.blob import BlockBlobService, PublicAccess
key ='WY3062K+hMvWGuZG2WlLVGtJNj1bi9************************************************/Xg8j1ITmmGg=='
accountname ='intelligentappstorage01'
block_blob_service = BlockBlobService(account_name=accountname,account_key=key)
block_blob_service.delete_container('audiofiles')
table_service = TableService(account_name=accountname, account_key=key)
table_service.delete_table('transcriptiontable')
for i in range(len(df)):
    chunk=df['ChunkNo'][i]
    starttime=df['Starttime'][i]
    endtime=df['Endtime'][i]
    filename = meetingID +"_" + "chunk" + str(chunk) + "_" + str(starttime) + "_" + str(endtime) + ".wav"
    os.remove(meetingID +"_" + "chunk" + str(chunk) + "_" + str(starttime) + "_" + str(endtime) + ".wav")

print("Local Audio Files Removed!")
########******************Sentiment Score*******************

import urllib.request
import json
 
# Configure API access
apiKey = 'f*****************d0'
sentimentUri = 'https://eastus.api.cognitive.microsoft.com/text/analytics/v2.0/sentiment'
# Prepare headers
headers = {}
headers['Ocp-Apim-Subscription-Key'] = apiKey
headers['Content-Type'] = 'application/json'
headers['Accept'] = 'application/json'

final_df["Sentiment"] = np.nan
for i in range(len(final_df['ConvertedText'])):
    time.sleep(1)
    Text = final_df['ConvertedText'][i]
    postData2 = json.dumps({"documents":[{"id":"1", "text":Text}]}).encode('utf-8')
    request2 = urllib.request.Request(sentimentUri, postData2, headers)
    response2 = urllib.request.urlopen(request2)
    response2json = json.loads(response2.read().decode('utf-8'))
    sentiment = response2json['documents'][0]['score'] 
    final_df['Sentiment'][i] = ('Sentiment: %f' % sentiment)
   
final_df.to_excel('result' + meetingID +'.xlsx')
print('Check the result')
result=final_df.drop(['PartitionKey', 'Timestamp', 'etag', 'RowKey', 'ChunkNo'], axis=1)
final_df['duration'] = final_df['Endtime'] - final_df['Starttime']
result=final_df.drop(['PartitionKey', 'Timestamp', 'etag', 'RowKey', 'ChunkNo','Endtime'], axis=1)
transcript_json=result.to_json(orient='records')
#Update JSON in SQL table

import pyodbc 
# & Drive can be downloaded from below link: 
# https://docs.microsoft.com/en-us/sql/connect/odbc/windows/system-requirements-installation-and-driver-files?view=sql-server-2017#installing-microsoft-odbc-driver-for-sql-server
server = 'tcp:intelligentappdb1.database.windows.net' 
database = 'intelligent_app' 
username = 'ayadav@intelligentappdb1' 
password = 'ABC@123xyz' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

qry = '''UPDATE dbo.CallDetails
        SET TranscriptJson = ?, TranscriptStatus = ?
        WHERE MeetingUrl = ?
        '''
param_values = [transcript_json, "Completed", meetingLink]
cursor.execute(qry, param_values)

print('{0} row updated successfully.'.format(cursor.rowcount))
 
cursor.commit() #Use this to commit the update operation
cursor.close()
cnxn.close()
#Remove intermittent folder
import shutil
shutil.rmtree('C:/demo/gomeetingframes'+ meetingID)
shutil.rmtree('C:/demo/crop_gomeetingframes' +meetingID)

