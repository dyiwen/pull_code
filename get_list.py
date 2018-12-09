# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 23:37:25 2017

"""
import sys, os, time
import traceback
import redis
from ftplib import FTP
from multiprocessing import cpu_count,Process,JoinableQueue,Queue

from FTP_ import FTPrsync, RsyncExtra

import logging 


#---------------------------------------------log----------------------------------------------------------------
def create_logging(filepath = 'watcher.log',mode = 'a' ):  #%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename = filepath,
                        filemode = mode)
    return logging

def out(s):
	logging.info("-"*25)
	logging.info(s)
	print(s)
#---------------------------------------------log----------------------------------------------------------------



def get_login(IP, PORT, USER, PWD, timeout=60):
    ftp = FTP()
    ftp.connect(IP,PORT,timeout = 60)
    ftp.login(USER,PWD)
    bufsize = 1024        #230:270  290 300:400
    datelist =  ftp.nlst()[297:298] #20170821-20171014 5917                 #270:290
    print datelist
    out(datelist)
    file_path_list = []

    for date_ in datelist:
        try:
            print date_+"/XA/"
            ftp.cwd("/"+date_+"/XA/")
            if len(ftp.nlst()) > 0:
                ftp_path = []
                for i in ftp.nlst():
                	ftp.cwd(i)
                	print len(ftp.nlst())
                	print "-"*50
                	if len(ftp.nlst()) < 350:
                		ftp_path.append(i)
                	ftp.cwd("../")
                print len(ftp_path)
                file_list = ["/"+date_+"/XA/"+j for j in ftp_path]	
                file_path_list.append(file_list)
            ftp.cwd("../")
        except:
            traceback.print_exc()

    print file_path_list
    print len(file_path_list)
    file_total = [s for l in file_path_list for s in l]
    print len(file_total)
    time.sleep(3)
    return file_total

# def get_list():
#     get_login(IP = "192.168.130.66",PORT=21,USER="xl",PWD="xl123")
#     print ftp.nlst()


def gdcmconv(source):
    """
    gdcm 解压dicom文件
    :param source: 数据源，dicom目录
    """
    for rootpath, folder, filenames in os.walk(source):
        for filename in filenames:
            filepath = os.path.join(rootpath, filename)
            cmd = "gdcmconv -w {} {}".format(filepath, filepath)
            os.system(cmd)


def service(source):

    try:
        f_path = source[0]
        t_path = source[1]
        print "-"*50
        print t_path
        print "-"*50
        out("start download image for patid {}".format(f_path))


        if True:
        	

        	ftpsync = RsyncExtra(user = 'xl', pwd = 'xl123', host = '192.168.130.66',port=21, timeout=60)
        	file_number = ftpsync.sync(f_path,t_path,tree=False)
        	

        # else:
        #     file_number = 0
        #     out("Choose a way that you want to achieve !")

        # if file_number > 0:
        # 	print "download image count {}".format(file_number, f_path)
        # else:
        # 	print "{} is zero".format(f_path)

        #     out("download image count {}".format(file_number, patient_id))
        #     redis_client.set(patient_id, outpath, ex=24*60*60*2)
        # else:
        #     out("the sending channl failed ,download image count {} patid is {}".format(file_number, patient_id))
    except:
        out("service run err {}".format(f_path))
        out(traceback.print_exc())


def Worker(q, total_number, cpuCount, root_number):
	start_time = time.time()
	n = 0
	while True:
		thread_time = time.time()
		item = q.get()
		if item is None:
			break
		service(item[1])
		n += 1

		use_time = (time.time() - start_time)/60
		thread_use_time = (time.time() - thread_time)
		# out("-"*25)
		current_number = len(os.listdir("/media/dyiwen/Elements/")) - root_number
		out("共有 {} 个进程,单次任务耗时 {} 秒".format(cpuCount,round(thread_use_time,3)))
		out("{} 完成了 {} 个，共有 {} 个任务".format(item[0],n,total_number))
		out("共耗时 {} 分钟,剩余 {} 个任务".format(round(use_time,3),total_number - current_number))
		# out("-"*25)
		q.task_done()


def Multi(redis_client, channel, patient_list):
    """
    开始获取符合条件的病人，进行预测
    :param redis_client:
    :param channel:
    :return:
    """
    try:
        starttime = time.time()
        cpuCount = cpu_count()  # 计算本机CPU核数
        # cpuCount = 3

        multiprocessing = []
        total_number = len(patient_list)
        root_number = len(os.listdir("/media/dyiwen/Elements/"))

        for i in xrange(0, cpuCount):
        	p = Process(target=Worker, args=(q,total_number,cpuCount,root_number))
        	p.daemon = True
        	p.start()
           	multiprocessing.append(p)

        global CNT
        for i in range(len(patient_list)):
            q.put([channel+str(CNT%cpuCount), patient_list[i]])
            CNT += 1
            
        q.join()

        for i in xrange(0, cpuCount):
            q.put(None)
        for p in multiprocessing:
            p.join()

        elapsed = (time.time() - starttime)
        out("cpuCount: {} Finished with time:{}".format(cpuCount, elapsed))
        # print "cpuCount: {} Finished with time:{}".format(cpuCount, elapsed)

    except:
        # print "Multi run error"
        # traceback.print_exc()
        out("Multi run error")
        out(traceback.print_exc())



def main(redis_client, channel):
    try:
    	# to_path = "/media/dyiwen/EEFC046BFC04307F/XL_AX/345_400_20170821_1014/"
        to_path = "/media/dyiwen/Elements/"
        patient_list = get_login(IP="192.168.130.66",PORT=21,USER="xl",PWD="xl123",timeout=60) # 查询patient dicom path (医院pacs)
        patient_list = [[i,os.path.join(to_path,os.path.split(i)[-1])] for i in patient_list]
        print patient_list
        out("current patient number {}".format(len(patient_list)))
        # print "current patient number {}".format(len(patient_list))
#-----------------------------------------------------------------------------------------
        if True:
            Multi(redis_client, channel, patient_list)
 
    except:
        out('main run error')
        out(traceback.print_exc())
        # out("main run error")
        # err(traceback.print_exc())

if __name__ == '__main__':
    try:
    	logging = create_logging('./log/watcher.log')
    	rcon = redis.Redis()
        channel = "进程"
        channelCount = 4
        CNT = 1
        
        while True:
            out("server run start !")
            # print('server run start !')
            q = JoinableQueue()
            main(rcon, channel)  # 测试两分钟无误后修改为False
            #time.sleep(float(config_obj.OTHER.server_sleep.value))
            # time.sleep(60)
            break
    except:
        print("run error")
        print(traceback.print_exc())
        # out("run error")
        # out(traceback.print_exc())

    
    


    
