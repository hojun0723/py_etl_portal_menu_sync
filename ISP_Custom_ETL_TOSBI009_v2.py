#BI포털 메뉴마스터 -> DW 동기화

import sqlite3
import pyodbc
#import MySQLdb
#import mysqlclient
#import pymysql
import datetime
# import threading
import sys
import time

def PGSQLtoMSSQL():

    try:
        # SOURCE SERVER INFO
        #C:\_Tab_Portal\database
        #cnxn = sqlite3.connect('C:/Users/Samsung/Desktop/바탕화면백업_180703/PIP_tableau_portal_180706.db')

        #yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
        #setfilename = yesterday.strftime('%Y-%m-%d')
        #setfilename = setfilename[:5] + str(int(setfilename[5:7])) + setfilename[7:]
        #dbname = 'PIP_tableau_portal_backup_' + setfilename + '.db'

        #cnxn = sqlite3.connect('C:/_Tab_Portal/database/' + dbname)
        #cursor = cnxn.cursor()

        cnxn = pyodbc.connect("Driver={PostgreSQL UNICODE};Server=123.123.123.123;Port=5432;Database=postgres;Uid=postgres;Pwd=password;")
        cursor = cnxn.cursor()

        # TARGET SERVER INFO
        # cnxn2 = pymysql.connect(host='123.123.123.123', user='root', passwd='password', database='RAW_POP', charset='utf8mb4') 
        # cursor2 = cnxn2.cursor(pymysql.cursors.DictCursor)
        cnxn2 = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=123.123.123.123;DATABASE=database;UID=userid;PWD=password')
        cursor2 = cnxn2.cursor()
		
        msg = "\nselect start: " + str(datetime.datetime.now())
        print(msg)

        # SQL
        # table  = "QLT_JAJOO"
        # ilja   = "2017-05-15"
        # addsql = " WHERE ILJA < '" + ilja + "'"
        # sql    = "SELECT * FROM " + table + addsql

        '''
        cursor.execute("""\
        SELECT PJI.*, PJO.*, 
                CST.ABCGS 
        FROM  dbo.Traverse AS TRE 
                      LEFT OUTER JOIN dbo.TraversePreEntry AS TPE 
                            ON TRE.JobNum = dbo.GetJobNumberFromGroupId(TPE.GroupId)
                      LEFT OUTER JOIN AutoCADProjectInformation AS PJI
                            ON TRE.JobNum = PJI.JobNumber
                      LEFT OUTER JOIN CalculationStorageReplacement AS CST
                            ON CST.ProjectNumber = dbo.GetJobNumberFromGroupId(TPE.GroupId)
                      LEFT OUTER JOIN dbo.TraverseElevations AS TEV
                          ON TRE.TraverseId = TEV.TraverseId
                      LEFT OUTER JOIN VGSDB.dbo.ProjectOffice PJO
                            ON PJI.PjbId = PJO.PjbId
        where jobnum = 1205992""")
        '''

        sql = ""
        sql = sql + """\
        /*
        select 	a.pmid, a.mid, a.mnm, b.mnm, c.mnm, d.mnm, e.mnm, f.mnm,

                        case when c.mnm is null then a.mnm || ';' || b.mnm
                             when d.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm
                             when e.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm || ';' || d.mnm
                             when f.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm || ';' || d.mnm || ';' || e.mnm
                        end  as mnm_full,		     

                        case when c.mnm is null then b.mid
                             when d.mnm is null then c.mid
                             when e.mnm is null then d.mid
                             when f.mnm is null then e.mid
                        end  as mid_full		
                        
        from   	menu_master a left outer join menu_master b on a.mid = b.pmid
                                                  left outer join menu_master c on b.mid = c.pmid
                                                  left outer join menu_master d on c.mid = d.pmid
                                                  left outer join menu_master e on d.mid = e.pmid
                                                  left outer join menu_master f on e.mid = f.pmid
        where  a.pmid = 0
        order  by a.morder, b.morder, c.morder, d.morder, e.morder, f.morder


        select          case when c.mnm is null then b.mid
                             when d.mnm is null then c.mid
                             when e.mnm is null then d.mid
                             when f.mnm is null then e.mid
                        end  as YMN_ID,

         	        case when c.mnm is null then a.mnm || ';' || b.mnm
                             when d.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm
                             when e.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm || ';' || d.mnm
                             when f.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm || ';' || d.mnm || ';' || e.mnm
                        end  as YMN_NM,		     
                        
                        STRFTIME("%Y%m%d",'now', 'localtime') AS CRT_DT,
                        'ISP' AS CRT_USER_ID,
                        DATETIME('now', 'localtime') AS DATA_CRT_DTM
                        
        from   	menu_master a left outer join menu_master b on a.mid = b.pmid
                                                  left outer join menu_master c on b.mid = c.pmid
                                                  left outer join menu_master d on c.mid = d.pmid
                                                  left outer join menu_master e on d.mid = e.pmid
                                                  left outer join menu_master f on e.mid = f.pmid
        where  a.pmid = 0
        order  by a.morder, b.morder, c.morder, d.morder, e.morder, f.morder
        */

        select 
                   case when a.mnm is not null and b.mnm is null then a.mid
                        when b.mnm is not null and c.mnm is null then b.mid
                        when c.mnm is not null and d.mnm is null then c.mid
                        when d.mnm is not null and e.mnm is null then d.mid
                   else e.mid end ymn_id,
                   
                   case when a.mnm is not null and b.mnm is null then a.mnm
                        when b.mnm is not null and c.mnm is null then a.mnm || ';' || b.mnm
                        when c.mnm is not null and d.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm
                        when d.mnm is not null and e.mnm is null then a.mnm || ';' || b.mnm || ';' || c.mnm || ';' || d.mnm
                   else a.mnm || ';' || b.mnm || ';' || c.mnm || ';' || d.mnm || ';' || e.mnm end ymn_nm,
                   
                   to_char(now(), 'yyyymmdd') crt_dt,
                   'ISP' crt_user_id,
                   now() data_crt_dtm

        from (
                        select *
                        from   menu_master
                        where  mlevel = 1
                 ) a left outer join (
                        select *
                        from   menu_master
                        where  mlevel = 2
                 ) b on a.mid = b.pmid
                 left outer join (
                        select *
                        from   menu_master
                        where  mlevel = 3
                 ) c on b.mid = c.pmid	     
                 left outer join (
                        select *
                        from   menu_master
                        where  mlevel = 4
                 ) d on c.mid = d.pmid
                 left outer join (
                        select *
                        from   menu_master
                        where  mlevel = 5
                 ) e on d.mid = e.pmid	 
                 
        order by a.morder, b.morder, c.morder, d.morder, e.morder

        """

        r = cursor.execute(sql)
        c = [column[0] for column in r.description]
        results = []
        
        k = 0
        for row in cursor.fetchall():

            k += 1
            results.append(dict(zip(c, row)))

            #print(row)

        msg = "%s row(s)" % (str(k))
        print(msg)
        
        msg = "select end  : " + str(datetime.datetime.now())
        print(msg)
        
        msg = "\ninsert start: " + str(datetime.datetime.now())
        print(msg)

        k = 0
        for myDict in results:
            
            k += 1
            columns = ','.join(myDict.keys())

            # %s 인 경우에는 에러 발생하여 ? 로 placeholders 변경
            placeholders = ','.join(['?'] * len(myDict))
	    #print(placeholders)

            sql = "insert into " + "TOSBI009" + " (%s) values (%s)" % (columns, placeholders)

            #print(sql)
            #return

            #print(sql)
            #print(list(myDict.values()))
            #return
            
            cursor2.execute(sql, list(myDict.values()))
            
            #sys.stdout.write("#")
            #sys.stdout.flush()

            #msg = "%s row(s)" % (str(k))
            #print(msg, end='')
            #print("\r", end='')

            
        msg = "%s row(s)" % (str(k))
        print(msg)

        msg = "insert end  : " + str(datetime.datetime.now())
        print(msg)
        
        cnxn2.commit()

        #time.sleep(5)
        
    except Exception as e:
        print('error:', e)

PGSQLtoMSSQL()
