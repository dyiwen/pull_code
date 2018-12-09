# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 23:37:25 2017

@author: shice
"""
import sys, os, time
import traceback
from ftplib import FTP
# from multiprocessing import cpu_count,Process,JoinableQueue,Queue

from FTP_ import FTPrsync, RsyncExtra


import multiprocessing

def writer_proc(q):      
    try:         
        q.put(2, block = False) 
    except:         
        pass   

def reader_proc(q):      
    try:         
        print q.get(block = False) 
    except:         
        pass

def Multi():

    writer = multiprocessing.Process(target=writer_proc, args=(q,))  
    writer.start()   

    reader = multiprocessing.Process(target=reader_proc, args=(q,))  
    reader.start()  

    reader.join()  
    writer.join()



def main():
    try:
        patient_list = get_login(IP="192.168.130.66",PORT=21,USER="xl",PWD="xl123",timeout=60) # 查询patient dicom path (医院pacs)
        patient_list = [s for i in patient_list for s in i]
        print patient_list
        # out("current patient number {}".format(len(patient_list)))
        print "current patient number {}".format(len(patient_list))
#-----------------------------------------------------------------------------------------
        if True:
            Multi(patient_list)
 
    except:
        print 'main run error'
        traceback.print_exc()
        # out("main run error")
        # err(traceback.print_exc())


def get_login(IP, PORT, USER, PWD, timeout=60):
    ftp = FTP()
    ftp.connect(IP,PORT,timeout = 60)
    ftp.login(USER,PWD)
    bufsize = 1024
    datelist =  ftp.nlst()[:100]
    print datelist
    file_path_list = []
    for date_ in datelist:
        try:
            print date_+"/XA/"
            ftp.cwd("/"+date_+"/XA/")
            if len(ftp.nlst()) > 0:
                file_list = ["/"+date_+"/XA/"+i for i in ftp.nlst()]
                file_path_list.append(file_list)
            ftp.cwd("../")
        except:
            pass
    print file_path_list
    print len(file_path_list)
    return file_path_list




if __name__ == "__main__":
    q = multiprocessing.Queue()
    main()
