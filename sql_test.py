#!/usr/bin/env python
# encoding: utf-8
import DAL
import json

def show_china(s):
	print json.dumps(s, indent = 0,ensure_ascii = False)	

#----------------------------------------------------------------------------------------------------

def select_id_list_from_sqls(TEST):
	con_sqls = DAL.sqlserver('192.168.130.113','txkj','txkj')
	#sql = "select RIS_ID,CheckPart,CONVERT(varchar(100),EXAM_DATE,112) from v_TX_PACS where EXAM_DATE >= convert(varchar(8),getdate(),112) and (CheckPart like '%上腹%' or CheckPart like '%胸%' or CheckPart like '%全腹%')"
	sql = "select RIS_ID,EXAM_PARTSUB_NAME from XinLuo_FC where EXAM_DATE between '2017-02-02 00:00:00' and '2017-04-01 00:00:00'"
	rowcount, result = con_sqls.execute(sql)
	con_sqls.close()
	#print result,'-------------'+str(len(result))
	#show_china(result)
	Ris_list = [i[0] for i in result]
	#print Ris_list,len(Ris_list)
	return Ris_list

def select_path_from_db2(ris_list):
	con_db2 = DAL.DB2('IPACSDB','192.168.130.66','50000','db2inst1','db2inst1')
	if con_db2:
		Ris_list = select_id_list_from_sqls(False)
		Patient_info_list = []
		for ris_id in Ris_list:
			sql = "select SERIES_UID,to_char(STUDY_DATE,'yyyymmdd') from PACS.TX_PACS where PATIENT_ID = '{}'".format(ris_id)
			result = con_db2.execute(sql)
			# Patient_info_list.append(result)
			if len(result) > 0:
				series_list = [s for s in result]
				series_id_list =[i[1]+'/XA/'+i[0] for i in series_list]
				Patient_info_list.append(i[1]+'/XA/'+i[0])
				# patient_info = [s[0],series_list]
				# Patient_info_list.append(series_list)
	print Patient_info_list		
	print len(Patient_info_list)
	con_db2.close()
	return Patient_info_list

def sql_t():
	con_db2 = DAL.DB2('IPACSDB','192.168.130.66','50000','db2inst1','db2inst1')
	if con_db2:
		#sql = "select * from PACS.TX_PACS where PATIENT_ID = '63023596' and to_char(STUDY_DATE,'yyyy-mm-dd') >= CURRENT DATE"
		sql = "select to_char(STUDY_DATE,'yyyy-mm-dd') from PACS.TX_PACS where PATIENT_ID = '63023596'"
		result = con_db2.execute(sql)
		print result
	con_db2.close()



if __name__ == '__main__':
	#select_id_list_from_sqls(False)
	select_path_from_db2(select_id_list_from_sqls(False))
	#sql_t()

