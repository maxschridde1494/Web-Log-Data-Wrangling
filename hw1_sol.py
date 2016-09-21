# ### * BEGIN STUDENT CODE *

# In[4]:

import apachetime
import time

def apache_ts_to_unixtime(ts):
    """
    @param ts - a Apache timestamp string, e.g. '[02/Jan/2003:02:06:41 -0700]'
    @returns int - a Unix timestamp in seconds
    """
    dt = apachetime.apachetime(ts)
    unixtime = time.mktime(dt.timetuple())
    return int(unixtime)


# In[5]:

import csv

def process_logs(dataset_iter):
    """
    Processes the input stream, and outputs the CSV files described in the README.    
    This is the main entry point for your assignment.
    
    @param dataset_iter - an iterator of Apache log lines.
    """
    # FIX ME
    with open("hits.csv", "w+") as hits_file:        
        writer = csv.writer(hits_file, lineterminator = '\n')
        writer.writerow(["ip", "timestamp"])
        for i, line in enumerate(dataset_iter):            
            logSplitList = line.split(" ")
            ipAddress = logSplitList[0]
            unixTimeStamp = apache_ts_to_unixtime(logSplitList[3] + " " + logSplitList[4])
            writer.writerow([ipAddress, str(unixTimeStamp)])
    
    get_ipython().system(u"sort -k1,1 -k2,2n -t',' hits.csv > intermediate.csv")
    
    with open("intermediate.csv", "r") as intermediate_file:
        with open ("sessions.csv", "w+") as sessions_file:
            reader = csv.reader(intermediate_file)
            writer = csv.writer(sessions_file, lineterminator = '\n')
            writer.writerow(["ip", "session_length", "num_hits"])
            lastIp = ""
            lastTimeStamp = 0
            first_hit = 0
            num_hits = 1
            checkWriteLastLine = False
            for row in reader:
                currentIp = row[0]
                if currentIp == "ip":
                    continue
                currentTimeStamp = int(row[1])
                
                #same machine
                if currentIp == lastIp:
                    #same session (within 30 min of last hit)
                    if currentTimeStamp - lastTimeStamp <= 1800:
                        num_hits += 1
                        checkWriteLastLine = False
                    else:
                        #same machine but start of new session
                        writer.writerow([currentIp, str(lastTimeStamp - first_hit), str(num_hits)])
                        checkWriteLastLine = True
                        num_hits = 1
                        first_hit = currentTimeStamp
                #new machine
                else:
                    if lastIp == "" and lastTimeStamp == 0:
                        first_hit = currentTimeStamp
                        num_hits = 1
                    else:    
                        writer.writerow([lastIp, str(lastTimeStamp - first_hit), str(num_hits)])
                        checkWriteLastLine = True
                        num_hits = 1
                        first_hit = currentTimeStamp


                lastIp = currentIp
                lastTimeStamp = currentTimeStamp
            if checkWriteLastLine == False:
                writer.writerow([lastIp, str(lastTimeStamp - first_hit), str(num_hits)])
    
    
    #sort sessions.csv based on session_length
    get_ipython().system(u"sort -k2,2n -t',' sessions.csv > intermediate2.csv")
    
    with open("intermediate2.csv", "r") as intermediate_file:
        with open("session_length_plot.csv", "wb") as session_length_plot_file:
            reader = csv.reader(intermediate_file)
            writer = csv.writer(session_length_plot_file, lineterminator = '\n')
            writer.writerow(["left", "right", "count"])
            counter = 0
            lowerBound = 0
            upperBound = 2
            for row in reader:
                if row[1] == "session_length":
                    continue
                session_length = int(row[1])
                if session_length >= lowerBound and session_length < upperBound:
                    #correct range
                    counter += 1
                else:
                    writer.writerow([str(lowerBound), str(upperBound), str(counter)])
                    counter = 1
                    while upperBound <= session_length:
                        lowerBound = upperBound
                        upperBound = lowerBound * 2
            writer.writerow([str(lowerBound), str(upperBound), str(counter)])


# ### * END STUDENT CODE *
import os
DATA_DIR = os.environ['MASTERDIR'] + '/sp16/hw1/'

import zipfile

def process_logs_large():
    """
    Runs the process_logs function on the full dataset.  The code below 
    performs a streaming unzip of the compressed dataset which is (158MB). 
    This saves the 1.6GB of disk space needed to unzip this file onto disk.
    """
    with zipfile.ZipFile(DATA_DIR + "web_log_large.zip") as z:
        fname = z.filelist[0].filename
        f = z.open(fname)
        process_logs(f)
        f.close()
process_logs_large()
