#!/usr/bin/env python
# encoding: utf-8

from multiprocessing import cpu_count,Process,JoinableQueue
import os, json, time, traceback
import shutil



def service():
	root_path = "/media/IPACSImage/jpegP10"
	date_list = os.listdir(root_path)
	print date_list

	to_path = "/media/dyiwen/EEFC046BFC04307F2/XL_AX/"
	n = 0
	b = 0
	starttime = time.time()
	if len(os.listdir("/media/dyiwen/EEFC046BFC04307F2/XL_AX/XA")) <= 10000 and len(os.listdir("/media/dyiwen/EEFC046BFC04307F2/XL_AX/")) <= 10000:
		for date_ in date_list:
			root_t_path = os.path.join(root_path,date_)
			#print root_t_path
			try:
				if os.path.exists(os.path.join(root_t_path,"XA")):
					for roots, dirs, files in os.walk(os.path.join(root_t_path,"XA")):
						if files > 0 :
							thrad_start = time.time()
							print roots
							# print os.path.split(roots)
							print os.path.join(to_path,os.path.split(roots)[1])
							if not os.path.exists(os.path.join(to_path,os.path.split(roots)[1])):

								cmd = "cp -rf {} {}".format(roots,to_path)
								result = os.popen(cmd).read()
								print result
								n += 1
								print 'Done',n
								thread = (time.time() - thrad_start)
								elapsed = (time.time() - starttime)/60
								print "共耗时 {} 分钟".format(elapsed)
								print "单次任务耗时 {}".format(thread)
								print "共移动 {} 个文件".format(len(os.listdir(os.path.join(to_path,os.path.split(roots)[1]))))
								cmd2 = "df -h | grep EEFC046BFC04307F2"
								print os.popen(cmd2).read()
							
							else:
								b += 1
								print "{} has been there {}".format(os.path.split(roots)[1],b)+"-"*50
								continue	
			except:
				traceback.print_exc()







# def Multi():
#     """
#     开始获取符合条件的病人，进行预测
#     :param redis_client:
#     :param channel:
#     :return:
#     """
#     try:
#         starttime = time.time()
#         cpuCount = cpu_count()  # 计算本机CPU核数


#         for i in xrange(0, cpuCount):  # 创建cpu_count()个进程
#             p = Process(target=service)
#             p.daemon = True
#             p.start()



#         elapsed = (time.time() - starttime)
#         # out("cpuCount: {} Finished with time:{}".format(cpuCount, elapsed))
#         print "cpuCount: {} Finished with time:{}".format(cpuCount, elapsed)

#     except:
#         print "Multi run error"


if __name__ == '__main__':
	service()