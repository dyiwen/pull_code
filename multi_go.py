#!/usr/bin/env python
# encoding: utf-8
"""
Copyright (c) 2015-2017 by Infervision, Inc
@author: shice
@contact: wshice@infervision.com
@file: server.py
@time: 17-6-30 下午1:46
@desc:
"""

import os, json, time, traceback
# import redis
# from sql_test import *
# from tools.SYSTEM.os_ import *
# from tools.SYSTEM.threading_ import  *
#from tools.EXT.filter import Dicomfilter
# from pulldata import *
#from constants import config_obj, out, err
from multiprocessing import cpu_count,Process,JoinableQueue
from get_list import get_login

CNT = 1 


def service(redis_client, channel, patient,TEST):
    """
    预测当前病人
    :param redis_client:
    :param channel:
    :param patient:
    """
    try:
        patient_id = patient[0]
        #out("start download image for patid {}".format(patient_id))
        #out("病人基本信息插入TXDB {}".format(patient_id))
        outpath = os.path.join(config_obj.STORE.dicom_store.value, patient_id)  # 文件存储地址
        print outpath
            # 四种取病人dicom方式，四选一
        if False:
            dicompath = patient[1]
            file_number = ftp_download(config_obj.FTP, dicompath, outpath)  # FTP 下载病人dicom
        elif False:
            dicompath = patient[1]
            file_number = cp_mount(dicompath, outpath)  # copy 挂载PACS存储
        else:
            file_number = 0
            out("Choose a way that you want to achieve !")

        if file_number > 0: # number > X
            out("download image count {}".format(file_number, patient_id))
            redis_client.set(patient_id, outpath, ex=24*60*60*2)
        else:
            out("the sending channl failed ,download image count {} patid is {}".format(file_number, patient_id))
    except:
        out("service run err {}".format(patient_id))
        err(traceback.print_exc())


def Worker(redis_client,TEST):
    while True:
        item = q.get()
        if item is None:
            break
        service(redis_client,item[0],item[1],TEST)
        q.task_done()

def Multi(redis_client, channel, patient_list,TEST):
    """
    开始获取符合条件的病人，进行预测
    :param redis_client:
    :param channel:
    :return:
    """
    try:
        starttime = time.time()
        cpuCount = cpu_count()  # 计算本机CPU核数
        multiprocessing = []

        for i in xrange(0, cpuCount):  # 创建cpu_count()个进程
            p = Process(target=Worker, args=(redis_client, TEST))
            p.daemon = True
            p.start()
            multiprocessing.append(p)

        global CNT
        for i in range(len(patient_list)):
            q.put([channel+str(CNT%channelCount), patient_list[i]])
            CNT += 1
        q.join()

        for i in xrange(0, cpuCount):
            q.put(None)
        for p in multiprocessing:
            p.join()

        elapsed = (time.time() - starttime)
        # out("cpuCount: {} Finished with time:{}".format(cpuCount, elapsed))
        print "cpuCount: {} Finished with time:{}".format(cpuCount, elapsed)

    except:
        print "Multi run error"
        # out("Multi run error")
        # err(traceback.print_exc())

#------------------------------------------------------------------------------------------
def get_patient_dicom_path(redis_client, patient_list):
    """
    遍历患者集合，匹配对应DICOM路径
    :param patient_list
    :return new_patient_list [[patient_id,dicom_path]...]  or [[patient_id,[dicom_path,...]]...]
    """
    new_patient_list = select_path_from_db2(patient_list)
    # for patient in patient_list:
    #     patient_id = patient[0]
    #     if not redis_client.exists(patient_id):
    #         dicompath = select_pacs_exam_path_per_id(patient_id)  # 查询dicom路径(医院PACS）
    #         #dicompath = patient[5]
    #         new_patient_list.append([patient_id, dicompath])
    return new_patient_list
#--------------------------------------------------------------------------------------------

def main():
    try:
        patient_list = get_login() # 查询patient dicom path (医院pacs)
        print patient_list
        # out("current patient number {}".format(len(patient_list)))
        print "current patient number {}".format(len(patient_list))
#-----------------------------------------------------------------------------------------
        if len(patient_list) < 0:
            Multi(redis_client, channel, patient_list,TEST) #start 
    except:
        print 'main run error'
        traceback.print_exc()
        # out("main run error")
        # err(traceback.print_exc())

if __name__ == '__main__':
    try:
        while True:
            #out("server run start !")
            print('server run start !')
            q = JoinableQueue()
            main()  # 测试两分钟无误后修改为False
            #time.sleep(float(config_obj.OTHER.server_sleep.value))
            time.sleep(60)
            break
    except:
        print("run error")
        print(traceback.print_exc())
        # out("run error")
        # out(traceback.print_exc())
