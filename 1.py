
# coding: utf-8

# # Edge VM

# In[ ]:


#Download meeting details from container

from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
account_name='intelligentappstorage01'
account_key='********************************'
block_blob_service = BlockBlobService(account_name, account_key)

generator = block_blob_service.list_blobs('meetingdetails-edgevm')

local_path =r"C:\demo\meetingLink.txt"
block_blob_service.get_blob_to_path('meetingdetails-edgevm',"meetingLink.txt", local_path)
local_path =r"C:\demo\meetingTime.txt"
block_blob_service.get_blob_to_path('meetingdetails-edgevm',"meetingTime.txt", local_path)


# In[ ]:


#Read meeting details
with open('meetingTime.txt', 'r') as myfile:
    meetingTime=myfile.read().replace('\n', '')

with open('meetingLink.txt', 'r') as myfile:
    meetingLink=myfile.read().replace('\n', '')
meetingID=meetingLink.rsplit('/', 1)[1]

video_name= 'gomeeting' + meetingID + ".mp4"


# In[ ]:


#Open meeting Link
from shutil import copyfile
src = "C:\\GoToMeeting Opener.exe"
dst = "C:\\Demo\\GoToMeeting Opener.exe"
copyfile(src, dst)
import webbrowser
import os
os.system('"GoToMeeting Opener.exe"')
ie = webbrowser.get("C:/Program Files (x86)/Internet Explorer/iexplore.exe %s")
ie.open(meetingLink)


# In[59]:


#find ongoing meeting
import subprocess
while True:
    processes = subprocess.Popen('tasklist', stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE).communicate()[0]
    go2meeting= b"g2mui.exe" in processes
    if go2meeting==True:
        break     


# In[ ]:


#Record Screen with audio----> (All device can be checked using script----> "ffmpeg -list_devices true -f dshow -i dummy"")
#Download audio capturer from "https://github.com/rdp/screen-capture-recorder-to-video-windows-free"
import time
import subprocess
subprocess.call(('ffmpeg -f dshow -i audio="virtual-audio-capturer" -f dshow  -framerate 2 -i video=screen-capture-recorder -rtbufsize 50 -vf scale=2560:1440 -vcodec libx264 -crf 0 -pix_fmt yuv420p -tune zerolatency -preset ultrafast -f mpegts -t %s "%s"')%(meetingTime, video_name),shell=True)
time.sleep(5)


# In[ ]:


#upload recording file in container
import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess
block_blob_service = BlockBlobService(account_name='intelligentappstorage01', account_key='****************************TmmGg==') 
container_name = "meetingrecordings"
block_blob_service.create_container(container_name)    #(for creating new container)
# Set the permission so the blobs are public.
block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
local_path=os.path.expanduser(r"C:\demo")
full_path_to_file =os.path.join(local_path, video_name)
block_blob_service.create_blob_from_path(container_name, video_name, full_path_to_file)


# In[ ]:


#message to service bus
from azure.servicebus import ServiceBusService, Message, Queue
bus_service = ServiceBusService(service_namespace='SVDServiceBus', shared_access_key_name='RootManageSharedAccessKey',shared_access_key_value='g********************p9+12EQu+*************=')
bus_service.create_queue('resultqueue')
msg = Message(video_name)
bus_service.send_queue_message('resultqueue', msg)


# In[ ]:


import os
import subprocess
os.remove(video_name)
#Close all running task
subprocess.call('taskkill.bat')

#Logout
import os
os.system("shutdown -l")


