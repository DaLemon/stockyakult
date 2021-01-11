import requests
import pymysql
import pyodbc
from bs4 import BeautifulSoup
from queue import Queue
import threading
import time
import datetime as dt

stock_info=[]
db_settings = {
    "host": "**********",
    "port": 3306,
    "user": "root",
    "password": "**********",
    "db": "**********",
    "charset": "utf8"
}
    # 建立Connection物件
conn = pymysql.connect(**db_settings)
    # 建立Cursor物件
cursor =conn.cursor()                            
mutex = threading.Lock()

def Catch_Data(all_stock_info,Stocknum_q):
	#將陣列4等分
	while Stocknum_q.qsize():
		#進入目標網頁
		url = 'http://pelements.money-link.com.tw/service/RQD2Service.ashx?systex=on&symid=%s'%Stocknum_q.get()
		res = requests.get(url)
		soup = BeautifulSoup(res.text , 'html.parser')
		#soup處理分割,存入陣列
		doc3 = str(soup).replace('"','')
		doc3 = doc3.split(',',20)
		stocknum = doc3[0].replace('{SID:','')
		#stock_info.append(doc3[1].replace('Name:',''))
		_date = '2020/%s'%doc3[3].replace('Date:','')
		Open = doc3[6].replace('O:','')
		high = doc3[7].replace('H:','')
		low = doc3[8].replace('L:','')
		close = doc3[9].replace('C:','')
		volume = doc3[16].replace('Qt:','')
		all_stock_info.append(['',stocknum,_date,Open,high,low,close,volume,None,None,None,None,None,None])
	return all_stock_info

def multithreading (all_stock_info):
	threads = []
	#建立4個程序
	for i in range(0,4):
		t=threading.Thread(target=Catch_Data,args=(all_stock_info,Stocknum_q))
		t.start()
		print('Catch_Data - Thread%d...STRAT'%i)
		threads.append(t)
	for thread in threads:
		thread.join()#等待線程結束後繼續
	print('Catch_Data - ALL Thread is FINISH')
	#result = []
	
	return all_stock_info

def KD_Calculator(stock_kd,Stocknum_q,dateid,RedDuckyYakult,RedConfirmDucky,RedYakult,Green2Yakult):
	while Stocknum_q.empty() == False:
		high_list = []
		low_list = []
		have_none = 0
		db_settings = {
		    "host": "**********",
		    "port": 3306,
		    "user": "root",
		    "password": "**********",
		    "db": "**********",
		    "charset": "utf8"
		}
		    # 建立Connection物件
		conn = pymysql.connect(**db_settings)
		    # 建立Cursor物件
		cursor =conn.cursor()

		command = "SELECT DateID,stockinfo.StockID,_Date,Open,High,Low,Close,Volume,K,D,EMA12,EMA26,DIF,MACD9,StockName,Market,Industry FROM stock.stockinfo,stock.stockid WHERE stockinfo.StockID=stockid.StockID AND DateID <= '%s' AND stockinfo.StockID = '%s' ORDER BY DateID DESC LIMIT 9"%(dateid,Stocknum_q.get())
		print(command)
		cursor.execute(command)
		for (DateID,StockID,_Date,Open,High,Low,Close,Volume,K,D,EMA12,EMA26,DIF,MACD9,StockName,Market,Industry) in cursor:
			high_list.append(High)
			low_list.append(Low)
			if DateID == dateid-3 :
				if K == None or D == None or EMA12 == None or EMA26 == None or MACD9 == None :
					pass
				else:
					DIF_3day_before = float(DIF)
					MACD9_3day_before = float(MACD9)
			elif DateID == dateid-2 :
				if K == None or D == None or EMA12 == None or EMA26 == None or MACD9 == None :
					pass
				else:
					K_before_yesterday = float(K)
					D_before_yesterday = float(D)
					EMA12_before_yesterday = float(EMA12)
					EMA26_before_yesterday = float(EMA26)
					DIF_before_yesterday = float(DIF)
					MACD9_before_yesterday = float(MACD9)
			elif DateID == dateid-1 :
				if K == None or D == None or EMA12 == None or EMA26 == None or MACD9 == None :
					have_none = 1
				else:
					C_yesterday = float(Close)
					K_yesterday = float(K)
					D_yesterday = float(D)
					EMA12_yesterday = float(EMA12)
					EMA26_yesterday = float(EMA26)
					DIF_yesterday = float(DIF)
					MACD9_yesterday = float(MACD9)
			elif DateID == dateid:
				C_today = float(Close)
				Sid = StockID
				_D = _Date
				O = Open
				H = High
				L = Low
				V = Volume
				Sname = StockName
				Market = Market
				Industry = Industry
		H_9day = max(high_list)
		L_9day = min(low_list)
		#漲幅
		percent = C_today/C_yesterday
		if percent > 1:
			PP = ('+'+str(round((percent-1)*100,2))+'%')
		elif percent == 1:
			PP = '0'
		else:
			PP = ('-'+str(round((1-percent)*100,2))+'%')
		#跳過昨日空白
		#1372(6289),1466(6598),1503(6732)
		if have_none == 1 or Sid ==6289 or Sid ==6598 or Sid ==6732 :
			stock_kd.append([dateid,Sid,_D,O,H,L,C_today,V,round(K,3),round(D,3),None,None,None,None])
			continue
		#RSV=(今日收盤價 - 最近九天的最低價)/(最近九天的最高價 - 最近九天最低價)
		#K = 2/3 X (昨日K值) + 1/3 X (今日RSV)
		#D = 2/3 X (昨日D值) + 1/3 X (今日K值)
		if H_9day == C_today:
			RSV = 100
		elif L_9day == C_today:
				RSV = 0
		else:
			RSV = 100*(C_today-L_9day)/(H_9day-L_9day)
		K = (2/3*K_yesterday) + (1/3*RSV)
		D = (2/3*D_yesterday) + (1/3*K)
		#print(RSV,K,D)
		#EMA12 = 昨日EMA12 * 11/13 + 今日收盤*2/13
		#DIF = EMA12 -EMA26
		#MACD9 = 昨日MACD * 8/10 +今日DIF *2/10
		EMA12 = EMA12_yesterday *11/13 +C_today*2/13
		EMA26 = EMA26_yesterday *25/27 +C_today*2/27
		DIF = EMA12 - EMA26
		MACD9 = MACD9_yesterday *8/10 + DIF*2/10		
		stock_kd.append([dateid,Sid,_D,O,H,L,C_today,V,round(K,3),round(D,3),round(EMA12,3),round(EMA26,3),round(DIF,3),round(MACD9,3)])
		#Filter 
		KD_before_yesterday_dispatch = K_before_yesterday - D_before_yesterday
		KD_yesterday_dispatch = K_yesterday - D_yesterday
		KD_today_dispatch = K - D
		OSC_3day_before = DIF_3day_before -MACD9_3day_before
		OSC_before_yesterday = DIF_before_yesterday - MACD9_before_yesterday
		OSC_yesterday = DIF_yesterday - MACD9_yesterday
		OSC_today = DIF - MACD9
		if KD_before_yesterday_dispatch > KD_yesterday_dispatch and KD_today_dispatch > KD_yesterday_dispatch and K_before_yesterday > K_yesterday and K > K_yesterday and KD_before_yesterday_dispatch > 0 and KD_yesterday_dispatch > 0 and KD_today_dispatch > 0 and  OSC_before_yesterday > OSC_yesterday and OSC_today > OSC_yesterday and OSC_today > 0 and OSC_yesterday > 0 and OSC_before_yesterday >0 and V>500:
			RedDuckyYakult.append([Sid,Sname,Market,Industry,C_today,PP])
		elif KD_before_yesterday_dispatch > KD_yesterday_dispatch and KD_today_dispatch > KD_yesterday_dispatch and K_before_yesterday > K_yesterday and K > K_yesterday and KD_before_yesterday_dispatch > 0 and KD_yesterday_dispatch > 0 and KD_today_dispatch > 0 and V>500:
			RedConfirmDucky.append([Sid,Sname,Market,Industry,C_today,PP])
		elif OSC_before_yesterday > OSC_yesterday and OSC_today > OSC_yesterday and OSC_today > 0 and OSC_yesterday > 0 and OSC_before_yesterday >0 and V>500:
			RedYakult.append([Sid,Sname,Market,Industry,C_today,PP])
		elif OSC_3day_before < OSC_before_yesterday and OSC_before_yesterday > OSC_yesterday and OSC_yesterday > OSC_today and OSC_3day_before < 0 and OSC_before_yesterday < 0 and OSC_yesterday < 0 and OSC_today < 0 and V>500:
			Green2Yakult.append([Sid,Sname,Market,Industry,C_today,PP])
				
		conn.close()
	return stock_kd,RedDuckyYakult,RedConfirmDucky,RedYakult


def multithreading2 (stock_kd,dateid,RedDuckyYakult,RedConfirmDucky,RedYakult,Green2Yakult):
	threads = []
	#建立4個程序
	for i in range(0,6):
		t=threading.Thread(target=KD_Calculator,args=(stock_kd,Stocknum_q,dateid,RedDuckyYakult,RedConfirmDucky,RedYakult,Green2Yakult))
		t.start()
		print('KD_Calculator - Thread%d...STRAT'%i)
		threads.append(t)
	for thread in threads:
		thread.join()#等待線程結束後繼續
	print('KD_Calculator - ALL Thread is FINISH')
	#result = []
	
	return stock_kd,RedDuckyYakult,RedConfirmDucky,RedYakult,Green2Yakult

def RemoveSpace(all_stock_info):

	for n in range(0,len(all_stock_info)):
		#去marketday抓DateID
		command = "SELECT DateID FROM marketday WHERE _Date = '%s'"%(all_stock_info[n][2])
		cursor.execute(command)
		dateid = cursor.fetchone()
		all_stock_info[n][0] = dateid[0]
		#判斷移除空格
		if all_stock_info[n][3] == '--':
			command = "SELECT * FROM stockinfo WHERE DateID = '%s' AND StockID = '%s'"%(all_stock_info[n][0]-1,all_stock_info[n][1])
			cursor.execute(command)
			a = cursor.fetchone()

			all_stock_info[n][3] = a[6]
			all_stock_info[n][4] = a[6]
			all_stock_info[n][5] = a[6]
			all_stock_info[n][6] = a[6]
			all_stock_info[n][7] = 0

	return all_stock_info

def WriteToDB(all_stock_info,dateid):

	command = "DELETE FROM stock.stockinfo WHERE DateID = '%s'"%(dateid)
	print(command)
	cursor.execute(command)
	conn.commit()

	command = "INSERT INTO stockinfo(DateID,StockID,_Date,Open,High,Low,Close,Volume,K,D,EMA12,EMA26,DIF,MACD9)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
	cursor.executemany(command,all_stock_info)
	conn.commit() 
	
	print('Finish....')

def UpLoadmssql(datetime,RedDuckyYakult,RedConfirmDucky,RedYakult,Green2Yakult):
	odb_settings = {
	    "DRIVER": "{SQL Server}",
	    "SERVER": "**********",
	    "UID": "**********",
	    "PWD": "**********",
	    "DATABASE": "**********",
	    "charset": "utf8"
	}
	O_cnxn = pyodbc.connect(**odb_settings)
	O_cursor = O_cnxn.cursor()
	#Detele old data
	command = "DELETE FROM dbo.UpdateTime"
	O_cursor.execute(command)
	command = "DELETE FROM dbo.RedYakult"
	O_cursor.execute(command)
	command = "DELETE FROM dbo.RedDucky"
	O_cursor.execute(command)
	command = "DELETE FROM dbo.RedDuckyYakult"
	O_cursor.execute(command)
	command = "DELETE FROM dbo.Green2Yakult"
	O_cursor.execute(command)



	command = "INSERT INTO dbo.UpdateTime(UTime) Values(?)"
	O_cursor.executemany(command,[datetime])
	O_cnxn.commit()
	if len(RedYakult)!=0:
		command = "INSERT INTO dbo.RedYakult(StockID,StockName,Market,Industry,Price,PricePercent) Values(?,?,?,?,?,?)"
		O_cursor.executemany(command,RedYakult)
		O_cnxn.commit()
	if len(RedConfirmDucky)!=0:
		command = "INSERT INTO dbo.RedDucky(StockID,StockName,Market,Industry,Price,PricePercent) Values(?,?,?,?,?,?)"
		O_cursor.executemany(command,RedConfirmDucky)
		O_cnxn.commit()

	if len(RedDuckyYakult)!=0:
		command = "INSERT INTO dbo.RedDuckyYakult(StockID,StockName,Market,Industry,Price,PricePercent) Values(?,?,?,?,?,?)"
		O_cursor.executemany(command,RedDuckyYakult)
		O_cnxn.commit()

	if len(Green2Yakult)!=0:
		command = "INSERT INTO dbo.Green2Yakult(StockID,StockName,Market,Industry,Price,PricePercent) Values(?,?,?,?,?,?)"
		O_cursor.executemany(command,Green2Yakult)
		O_cnxn.commit()

def main():
	all_stock_info = []
	global stock_kd,DuckyYakult,ConfirmDucky,Green2Yakult,datetime
	stock_kd = []
	RedDuckyYakult = []
	RedConfirmDucky = []
	RedYakult = []
	Green2Yakult =[]
	datetime = []
	global Stocknum_q,dateid
	now = dt.datetime.now()
	datetime.append(now.strftime("%H")+':'+now.strftime("%M"))
	print(datetime)
	Stocknum_q = Queue()
	command = "SELECT StockID FROM stockid"
	cursor.execute(command)
 	# 取出上市上櫃股票
 	
	for (StockID,) in cursor:
		Stocknum_q.put(StockID)
	print('Load DB Finish...')

	all_stock_info = multithreading(all_stock_info)#爬資料多呈緒
	all_stock_info = RemoveSpace(all_stock_info)
	
	dateid = all_stock_info[0][0]
	WriteToDB(all_stock_info,dateid)

	Stocknum_q = Queue()
	command = "SELECT StockID FROM stockid"
	cursor.execute(command)
 	# 取出上市上櫃股票
	for (StockID,) in cursor:
		Stocknum_q.put(StockID)
	multithreading2(stock_kd,dateid,RedDuckyYakult,RedConfirmDucky,RedYakult,Green2Yakult)
	#本機MYSQL
	WriteToDB(stock_kd,dateid)
	#雲端MSSQL
	UpLoadmssql(datetime,RedDuckyYakult,RedConfirmDucky,RedYakult,Green2Yakult)
	

if __name__ == '__main__':
	main()