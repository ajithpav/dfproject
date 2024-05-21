from email import encoders
from email.mime.base import MIMEBase
import re
import uuid
from config.config import logging, config
import json
from commons.utils import dt_parse
from commons.enum import NamingSeries,BusinessArea,SupplierType,Constants
from datetime import datetime,timedelta
import requests
from commons.mariadb import SessionMDB
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from weasyprint import CSS, HTML

def file_status_updator(filename,filepath,att_status,comments):
	try:
		return f""" update tabFile set attachment_status = "{att_status}", modified = now(), additional_comments = "{comments}" where (file_name = "{filename}" or file_url = "{filepath}") and attachment_type = "UFPD" """
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in file_status_updator:', str(Ex))

def notification_status_updator(noti_name,status):
	try:
		if status == 'Success':
			noti_val = '<b style="color:#00cc00;">Success</b>'
		elif status == 'Failed':
			noti_val = '<b style="color:#ff3300;">Failed</b>'
		else:
			noti_val = '<b style="color:#6699ff;">Processing</b>'
		return f""" update `tabNotification Alert` set processing_status = '{noti_val}' where name = '{noti_name}' """
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in file_status_updator:', str(Ex))


		

#region credit limit ICICI

def process_bank_credit_files(session,file_data):
	try:        
		res = 'Failed'
		read = file_data.get('data','')
		owner = file_data.get('user_name','')
		country_name = file_data.get('country','')
		if read != '':
			data = json.loads(read) 
			cf_reset_query = f"update tabCompany set is_channelfinance_enabled = 0, is_channelfinance_enabled_p08 = 0, show_credit_limit = 0 where name in (select company from `tabCredit Limit` tcl where bank_name = '{Constants.ICICI_bank_name.value}' and business_area_account  not in {Constants.TestPayerCodes.value})"
			session.execute(cf_reset_query)
			for row in data:
				loan_account = re.sub('[^A-Za-z0-9]+', '',row['Account No'])
				if loan_account.strip() == 'Totals':
					break
				else:
					business_area_account = row['Dealer Code']
					credit_limit = row['Dealer Limit']
					limit_expiry_date = datetime.strptime(str(row['Dealer Expiry Date']), '%d-%b-%Y')
					utilization = float(row['Dealer Limit']) - float(row['Unutilized Limit - INR'])
					bank_total_overdue = float(row['Overdue Within Cure - INR']) + float(row['Overdue Beyond Cure - INR'])
					payment_terms_in_days = int(row['Normal Tenor'])
					if business_area_account.replace(' ','')!='':
						is_shared = 0 if ',' not in business_area_account else 1
						pay_q = f""" select distinct payer_code, external_system from `tabCompany SOH` tcs where payer_code in ({business_area_account}) """
						pay_res = session.execute(pay_q)
						if pay_res.rowcount > 0:
							for i in pay_res:
								business_area_account = i['payer_code']
								ext_sys = i['external_system']
								res = credit_data_processor(session,business_area_account,loan_account,credit_limit,limit_expiry_date,utilization,bank_total_overdue,is_shared,payment_terms_in_days,owner,ext_sys,country_name)                     
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in process_bank_credit_files method: ',Except)  
		res = 'Failed' 
	session.commit()
	return res

def credit_data_processor(session,business_area_account,loan_account,credit_limit,limit_expiry_date,utilization,bank_total_overdue,is_shared,payment_terms_in_days,owner,ext_sys,country_name):
	try:
		response = 'Success'
		supplier_type = Constants.Supplier_Type.value
		supplier = 'Bayer'
		supplier_type_desc = 'Bank'
		bank_name = Constants.ICICI_bank_name.value

		company_query = f""" select distinct parent from `tabCompany SOH` tcs where payer_code = '{business_area_account}' AND external_system = '{ext_sys}' limit 1 """
		company_res = (session.execute(company_query).fetchall())

		if len(company_res) > 0:
			company = company_res[0]['parent']
			check_query = f""" select name from `tabCredit Limit` tcl where business_area_account ='{business_area_account}' """
			check_res = session.execute(check_query).fetchall()

			if len(check_res) > 0:
				# print(f"Updating for {business_area_account} | {loan_account} | {credit_limit} | {utilization} | {company} ")
				update_query = f"""UPDATE `tabCredit Limit`
								SET modified=now(), loan_account='{loan_account}', credit_limit='{credit_limit}',bank_total_overdue='{bank_total_overdue}', last_updated_date_from_sap=now(), utilization='{utilization}', limit_expiry_date='{limit_expiry_date}',processed_limit_expiry_date = NULL, enabled=1, is_channel_finance=1, is_shared='{is_shared}', bank_name='{bank_name}'
								WHERE name='{check_res[0]['name']}';"""   
				session.execute(update_query)
			else:
				insert_query = f"""INSERT INTO `tabCredit Limit`
									(name, creation, modified, modified_by, owner, naming_series, company, supplier_type, business_area_account, supplier, supplier_type_desc, loan_account, credit_limit, bank_total_overdue, last_updated_date_from_sap, status, utilization, limit_expiry_date, enabled, is_channel_finance, is_shared, bank_name, external_system, business_area_desc,country)
									VALUES(uuid(), now(), now(), '{owner}', '{owner}', 'UUID', '{company}', '{supplier_type}','{business_area_account}','{supplier}', '{supplier_type_desc}', '{loan_account}', '{credit_limit}', '{bank_total_overdue}', now(), 'Active', '{utilization}', '{limit_expiry_date}', 1, 1, '{is_shared}', '{bank_name}','{ext_sys}',(select description from `tabExternal System` where name = "{ext_sys}"),'{country_name}');"""
				session.execute(insert_query) 
			update_company_cf(session,company,payment_terms_in_days,owner,ext_sys)
		else:
			print(f"Company with {business_area_account} does not exist in the system.")
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in credit_data_processor method: ',Except)   
		response = 'Failed'
	return response

def update_company_cf(session,company,payment_terms_in_days,owner,ext_sys=None):
	query = f"""UPDATE tabCompany SET 
				is_channelfinance_enabled_p08 = CASE
					WHEN '{ext_sys}' = '{BusinessArea.P08.value}' THEN 1
					ELSE is_channelfinance_enabled_p08
				END,
				is_channelfinance_enabled = CASE
					WHEN '{ext_sys}' = '{BusinessArea.PBC.value}' THEN 1
					ELSE is_channelfinance_enabled
				END,
				modified = NOW(),
				modified_by = '{owner}',
				show_credit_limit = 1,
				payment_terms_in_days = '{payment_terms_in_days}',
				customer_accepted_autopulling = '{payment_terms_in_days - 1}'
				where name = '{company}' 
			"""
	session.execute(query)

#endregion


#region credit limit AXIS || Taken directly from the old airflow so the code structure might be different.

class BankCreditUtilisation():
	pass
	jsondata = ''
	session = None
	def getSession(self):
		try:
			self.session = SessionMDB()
			return self.session
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in getSession method: ',Except)  
		

	def insertcreditlimitdump(self,data,owner,country_name):
			try:           
				if not self.session:
					self.session = self.getSession()
				for row in data:
					imname = row['IM NAME']
					dealername = row['DEALER NAME']
					dealeraccountno = row['DEALER ACCOUNT NO.']
					dealercode = str(row['DEALER CODE'])
					branchcode = int(row['BRANCH CODE'])
					smecenter = row['SME CENTER']
					sanctionlimit = row['SANCTION LIMIT']
					limitutilized = row['LIMIT UTILIZED']
					accountstatus = row['ACCOUNT STATUS']
					totaloverdueamount = row['TOTAL OVERDUE AMOUNT']
					overduedays = int(row['OVERDUE DAYS'])
					limitexpirydate = datetime.strptime(str(row['LIMIT EXPIRY DATE']),"%d/%m/%Y")
					formattedlimitexpirydate = limitexpirydate.strftime("%Y-%m-%d")
					ext_sys = ''

					if dealername != '':
						is_shared = 0 if ',' not in dealercode else 1
						pay_q = f"SELECT DISTINCT payer_code, external_system FROM `tabCompany SOH` tcs WHERE payer_code IN ({dealercode})"
						pay_res = self.session.execute(pay_q)
						if pay_res.rowcount > 0:
							for i in pay_res:
								dealercode = i['payer_code']
								ext_sys = i['external_system']
								self.dumpProcessor(sanctionlimit, limitutilized, accountstatus, overduedays, totaloverdueamount,
												imname, dealeraccountno, branchcode, dealername, dealercode, smecenter,
												formattedlimitexpirydate, is_shared,owner,ext_sys,country_name)

	
				message = "Success"                           
			except Exception as e:    
				message = "Exception"       
				Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
				print('Error in insertcreditlimitdump method: ',Except) 
			self.session.commit()
			return message

	def checkdumprowexist(self,im_name,dealer_account_no,branch_code,dealer_name,dealer_code,sme_center):
		try:
			query = f""" SELECT count(*) as rec from `tabBank Credit Utilisation Dump` WHERE im_name ='{im_name}' AND dealer_account_no ='{dealer_account_no}' AND branch_code ='{branch_code}' AND dealer_name ='{dealer_name}' AND dealer_code ='{dealer_code}' AND sme_center ='{sme_center}' """
			# print(query)
			result = self.session.execute(query)
			for x in result:       
				return x.rec      
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in checkdumprowexist method: ',Except) 

	def updatedumprow(self,sanction_limit,limit_utilized,account_status,overdue_days,total_overdue_amount,
	im_name,dealer_account_no,branch_code,dealer_name,dealer_code,sme_center,owner):
		
		try:
			query = f"""UPDATE `tabBank Credit Utilisation Dump` SET
						sanction_limit={sanction_limit},
						limit_utilized={limit_utilized},
						account_status ='{account_status}',
						overdue_days={overdue_days},
						total_overdue_amount={total_overdue_amount},
						modified_by = '{owner}',
						owner = '{owner}'
						WHERE im_name ='{im_name}'
						AND dealer_account_no ='{dealer_account_no}'
						AND branch_code ='{branch_code}'
						AND dealer_name ='{dealer_name}'
						AND dealer_code ='{dealer_code}'
						AND sme_center ='{sme_center}'"""
			self.session.execute(query)       
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in updatedumprow method: ',Except) 

	def dumprow(self,name,im_name,dealer_account_no,
	branch_code,dealer_name,dealer_code,sme_center,
	sanction_limit,overdue_days,limit_expiry_date,
	limit_utilized,total_overdue_amount,account_status,is_shared,owner,ext_sys,country_name):
		try:
			query=f"""INSERT INTO `tabBank Credit Utilisation Dump`
					(name, creation, modified, modified_by, 
					owner,  
					naming_series, im_name, 
					dealer_account_no, branch_code, dealer_name, dealer_code, 
					sme_center, sanction_limit, overdue_days, limit_expiry_date, 
					limit_utilized, total_overdue_amount, account_status,is_shared,country,external_system)
					VALUES('{name}', CURDATE(), CURDATE(), '{owner}', 
					'{owner}', 
					'BCUD-.#', '{im_name}', 
					'{dealer_account_no}', '{branch_code}', '{dealer_name}', '{dealer_code}', 
					'{sme_center}', {sanction_limit}, {overdue_days}, '{limit_expiry_date}', 
					{limit_utilized},{total_overdue_amount}, '{account_status}','{is_shared}','{country_name}','{ext_sys}');"""
			self.session.execute(query)       
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in dumprow method: ',Except)

	def insertcreditlimitfromdumptable(self,owner):
		try:
			if not self.session:
				self.session = self.getSession()
			suppliertype = SupplierType.Bank.value
			# currentvalue=common.getTabSeriesNumber(NamingSeries.CL.value,self.session)
			query = f"SELECT * from `tabBank Credit Utilisation Dump` where modified_by = '{owner}'"
			dumpresult = self.session.execute(query)
			for dump in dumpresult:
				c_name=dump.dealer_name 
				dealeraccountnum = dump.dealer_account_no
				dealercode = dump.dealer_code 
				branchcode = dump.branch_code
				smecenter = dump.sme_center 
				sanctionlimit = dump.sanction_limit
				limitutilized = dump.limit_utilized
				accountstatus = dump.account_status
				totaloverdueamount = dump.total_overdue_amount
				overduedays = dump.overdue_days
				limitexpirydate = dump.limit_expiry_date
				is_shared = dump.is_shared
				ext_sys = dump.external_system
				company = self.getcompanyname(dealercode,ext_sys)
				country_name = dump.country

				bank_name = 'Axis' #### This airflow is only used for files that are marked for Axis bank

				reccount=  self.checkrecordexist(company,dealeraccountnum,dealercode,branchcode,smecenter)
				if reccount > 0:
					self.updaterow(company,sanctionlimit,limitutilized,
					accountstatus,totaloverdueamount,overduedays,limitexpirydate,
					dealeraccountnum,dealercode,branchcode,smecenter,is_shared,bank_name,owner,ext_sys)
				else:
					prefixvalue = str(uuid.uuid4().hex) 
					companyname = self.getcompanyname(dealercode,ext_sys)
					if companyname.strip(' ') !='':
						self.insertrow(prefixvalue,companyname,suppliertype,
						dealercode,sanctionlimit,limitutilized,accountstatus,totaloverdueamount,
						dealeraccountnum,branchcode,overduedays,smecenter,limitexpirydate,is_shared,bank_name,owner,ext_sys,country_name)
			message = "Success"            
		except Exception as e:
			message = "Exception"
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in insertcreditlimitfromdumptable method: ',Except)
		finally:          
			return message
	
	def checkrecordexist(self,company,dealeraccountnum,dealercode,branchcode,smecenter):
		try:
			query = f"""SELECT count(*) as rec from `tabCredit Limit` 
					WHERE company = '{company}' 
					and business_area_account = '{dealercode}'
					AND supplier_type = '{SupplierType.Bank.value}';"""
			result = self.session.execute(query)
			for x in result:       
				return x.rec   
		except Exception as e:
			message = "Exception"
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in checkrecordexist method: ',Except)
	
	def updaterow(self,company,sanctionlimit,limitutilized,accountstatus,totaloverdueamount,
		overduedays,limitexpirydate,dealeraccountnumber,dealercode,branchcode,smecenter,is_shared,bank_name,owner,ext_sys):
		try:
			query=f"""UPDATE `tabCredit Limit`
					SET modified=now(), credit_limit={sanctionlimit}, 
					utilization={limitutilized}, 
					status='{accountstatus}', 
					last_updated_date_from_sap=now(), 
					bank_total_overdue='{totaloverdueamount}', 
					overdue_days={overduedays}, 
					limit_expiry_date='{limitexpirydate}',
					is_shared = '{is_shared}',
					bank_name = '{bank_name}',
					processed_limit_expiry_date = NULL
					WHERE company = '{company}'
					and business_area_account = '{dealercode}'
					AND supplier_type = '{SupplierType.Bank.value}' """
			self.session.execute(query)   
			self.update_company_cf(company,owner,ext_sys)
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in updaterow method: ',Except)

	def getcompanyname(self,dealercode,ext_sys):
		try:
			companyName = ''    
			companyquery=f"select parent from `tabCompany SOH` where payer_code = '{dealercode}' AND external_system = '{ext_sys}'"
			result = self.session.execute(companyquery)
			if result:
				for x in result:       
					companyName = x.parent if x and x.parent else ''
			return companyName
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in getcompanyname method: ',Except)

	def insertrow(self,name,company,suppliertype,businessareaaccount,
		creditlimit,utilization,accountstatus,banktotaloverdue,loanaccount,branchcode,overduedays,
		smecenter,limitexpirydate,is_shared,bank_name,owner,ext_sys,country_name):
			try:
				bank_name
				query=f"""INSERT INTO `tabCredit Limit`
							(name, creation, modified, modified_by, 
							owner,naming_series, company, 
							supplier, supplier_type,  business_area_account, 
							credit_limit, utilization, status, last_updated_date_from_sap, 
							bank_total_overdue, supplier_type_desc,  loan_account, 
							branch_code, overdue_days, sme_center, limit_expiry_date,is_shared,bank_name,external_system,business_area_desc,country)
							VALUES('{name}', CURDATE() , CURDATE() , '{owner}', 
							'{owner}', 'UUID', '{company}', 
							'Bayer', '{suppliertype}',  '{businessareaaccount}', 
							{creditlimit}, {utilization}, '{accountstatus}', CURDATE(), 
							{banktotaloverdue}, '{SupplierType.Bank.value}',  '{loanaccount}', 
							'{branchcode}', {overduedays}, '{smecenter}', '{limitexpirydate}','{is_shared}','{bank_name}','{ext_sys}',(select description from `tabExternal System` where name = "{ext_sys}"),'{country_name}');"""
				self.session.execute(query)  
				self.update_company_cf(company,owner,ext_sys)          
			except Exception as e:
				Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
				print('Error in insertrow method: ',Except)

	def truncateDataFromDumpTable(self,owner): #### deletes now
		message = ''
		try:
			query = f'delete from `tabBank Credit Utilisation Dump` where modified_by = "{owner}"'
			self.session.execute(query)
			message = "Success"
		except Exception as e:
			message = "Exception"
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in truncateDataFromDumpTable method: ',Except)
		return message
		
	def processBankCreditUtilisationData(self,file_data):
		try:
			message= 'Failed'
			result = ''
			read = file_data.get('data','')
			owner = file_data.get('user_name','')
			country_name = file_data.get('country','')
			if read != '':
				data = json.loads(read)
				result = self.insertcreditlimitdump(data,owner,country_name)
				if result == "Success":
					cf_reset_query = f"update tabCompany set is_channelfinance_enabled = 0, is_channelfinance_enabled_p08 = 0, show_credit_limit = 0 where name in (select company from `tabCredit Limit` tcl where bank_name = '{Constants.Axis_bank_name.value}' and business_area_account  not in {Constants.TestPayerCodes.value})"
					self.session.execute(cf_reset_query)
					print("Successfully inserted credit limit utilisation data to dump table and updated Companies")
					result = self.insertcreditlimitfromdumptable(owner)
					if result == "Success":
						print("Successfully inserted credit limit utilisation data to credit limit table")
						self.session.commit()
						result = self.truncateDataFromDumpTable(owner)
						if result == "Success":
							print("Successfully removed credit limit utilisation data from dump table")
							self.session.commit()
							message = result
			else:
				print("No new emails to process.")
				message= 'Failed'
		except Exception as e:
			if self.session:
				self.session.rollback()
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in processBankCreditUtilisationData method: ',Except)
		finally:
			if self.session:
				self.session.close()   
		return message             

	def dumpProcessor(self,sanctionlimit,limitutilized,accountstatus,overduedays,totaloverdueamount,imname,dealeraccountno,branchcode,dealername,dealercode,smecenter,formattedlimitexpirydate,is_shared,owner,ext_sys,country_name):
		try:
			recordcount = self.checkdumprowexist(imname,dealeraccountno,branchcode,dealername,dealercode,smecenter)
			if accountstatus == 'ACTIVE':
				accountstatus = 'Active'
			elif accountstatus == 'INACTIVE':
				accountstatus = 'Inactive'
			elif accountstatus == 'DORMANT/Debit Freeze':
				accountstatus = 'Dormant/Debit Freeze'
			elif accountstatus == 'DORMANT':
				accountstatus = 'Dormant'   

			if(recordcount > 0):
				self.updatedumprow(sanctionlimit,limitutilized,accountstatus,overduedays,
				totaloverdueamount,imname,dealeraccountno,branchcode,dealername,dealercode,smecenter,owner)
			else:
				nameseriesvalue = str(uuid.uuid4().hex) 
				self.dumprow(nameseriesvalue,imname,dealeraccountno,
				branchcode,dealername,dealercode,smecenter,sanctionlimit,
				overduedays,formattedlimitexpirydate,limitutilized,totaloverdueamount,
				accountstatus,is_shared,owner,ext_sys,country_name)   
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in dumpProcessor method: ',Except)
		
	
	def update_company_cf(self,company, owner,ext_sys):
		try:
			query = f""" UPDATE tabCompany SET 
						is_channelfinance_enabled_p08 = CASE
							WHEN '{ext_sys}' = '{BusinessArea.P08.value}' THEN 1
							ELSE is_channelfinance_enabled_p08
						END,
						is_channelfinance_enabled = CASE
							WHEN '{ext_sys}' = '{BusinessArea.PBC.value}' THEN 1
							ELSE is_channelfinance_enabled
						END,
						modified = NOW(),
						modified_by = '{owner}',
						show_credit_limit = 1,
						payment_terms_in_days = CASE
							WHEN payment_terms_in_days = 0 THEN 90
							ELSE payment_terms_in_days
						END,
						customer_accepted_autopulling = CASE
							WHEN customer_accepted_autopulling = 0 THEN 89
							ELSE customer_accepted_autopulling
						END
					Where name = '{company}' 
			"""
			# print(query)
			self.session.execute(query)   
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in update_company_cf method: ',Except)

#endregion


#region bank discount AXIS || Taken directly from the old airflow so the code structure might be different.

class BankDiscounting():
	pass
	jsondata = ''
	session = None

	def getSession(self):
		try:
			self.session = SessionMDB()
			return self.session
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in getSession method: ',Except)  


	def insertbankdiscountingdump(self,data,owner,country_name):
		try:           
			if not self.session:
				self.session = self.getSession()
		
			if data != '':
				try:
					for row in data:
						# print(row)
						print(f"Processing row {row['Sr No.']}")
						im_name = row['IM NAME']
						dealer_code = int(row['DEALER CODE'])
						invoice_no = row['INVOICE/INDENT NO.']
						indent_date = str(row['INVOICE/INDENT DATE'])
						indent_date = datetime.strptime(indent_date,'%d/%m/%Y')
						status = row['STATUS']
						status_description = row['STATUS DESCRIPTION']
						if str(invoice_no).isnumeric():
							record_count = self.checkdumprowexist(dealer_code, indent_date, invoice_no)
							if record_count > 0:
								self.updatedumprow(dealer_code, indent_date, invoice_no, status, status_description,owner)
							else:
								current_value = str(uuid.uuid4().hex)
								nameseries_value = NamingSeries.UUID.value
								self.dumprow(current_value, im_name, dealer_code, indent_date, invoice_no, status,
											status_description, nameseries_value,owner,country_name)
				except Exception as e:
					except_msg = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
					print('Error in insertbankdiscountingdump method, dump insert/update logic:', except_msg)
				   
			# common.updateTabSeries(NamingSeries.BDD,self.session,currentvalue)           
			message = "Success"                           
		except Exception as e:    
			message = "Exception"       
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in insertbankdiscountingdump-1 method: ',Except)        
			 
		return message


	def checkdumprowexist(self,dealercode,indent_date,invoice_no):
			try:
				query = f"""SELECT  COUNT(*)  FROM `tabBank Discounting Dump` tbdd WHERE dealer_code ='{dealercode}' and indent_date = '{indent_date}' and invoice_number = '{invoice_no}' """
				result = self.session.execute(query)
				for x in result: 
					return x[0]      
			except Exception as e:
				Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
				print('Error in checkdumprowexist method: ',Except)        

	def updatedumprow(self,dealercode,indent_date,invoice_no,status,status_description,owner):
		try:
			if not self.session:
				self.session = self.getSession()
			query = f""" UPDATE `tabBank Discounting Dump` Set modified = NOW(),status = "{status}",modified_by = "{owner}",owner = "{owner}",
			 status_description = "{status_description}" 
			 WHERE dealer_code ='{dealercode}' and indent_date ='{indent_date}' 
			 and invoice_number = '{invoice_no}'"""


			self.session.execute(query)       
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in updatedumprow method: ',Except)         
			

	def dumprow(self,name,im_name,dealercode,indent_date,invoice_no,status,status_description,naming_series,owner,country_name):
		try:
			if not self.session:
				self.session = self.getSession()
			query = f"""INSERT  into `tabBank Discounting Dump`(name,creation,modified,modified_by,owner,
						naming_series,im_name,dealer_code,indent_date,invoice_number,status,status_description,country)                  
						values('{name}',curdate(),curdate(),'{owner}','{owner}',
						'{naming_series}','{im_name}','{dealercode}',"{str(indent_date)}",'{invoice_no}','{status}','{status_description}','{country_name}') """
			self.session.execute(query) 
			self.session.commit()      
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in dumprow method: ',Except)        
			
	

	def update_invoice_payment(self,owner):
		try:
			result=''
			if not self.session:
				self.session = self.getSession()
			query = f"SELECT * from `tabBank Discounting Dump`"
			dumpresult=self.session.execute(query)
			for dump in dumpresult:
				dealer_code=dump.dealer_code
				indent_date =dump.indent_date
				indent_date= indent_date.strftime("%Y/%m/%d")
				status=dump.status
				status_desciption = dump.status_description
				invoice_number = dump.invoice_number

				reccount = self.checkrecordexist(dealer_code,status,status_desciption,invoice_number)
				if reccount > 0:
					result = self.updaterow(dealer_code,indent_date,status,status_desciption,invoice_number)
					if result == "Successfull":
						result1 = self.update_payment_table(invoice_number,dealer_code)
						if result1 == 'Success':
							print("update_payment_table Success")
							self.update_finance_entry(invoice_number,owner)
				else:
					print(f"There are no document exist to update constrained to : {invoice_number}")        
						
			message = "Success"            
		except Exception as e:
			message = "Exception"
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in update_invoice_payment method: ',Except)        
			
		finally:            
			return message
		
	def checkrecordexist(self,dealer_code,status,status_desciption,invoice_number):

		query = f"""SELECT COUNT(*) FROM `tabInvoice Payment` tip WHERE parent in 
		(select name from tabPayment 
		tp2 WHERE payer = '{dealer_code}' and CAST(invoice_number AS SIGNED) = CAST("{invoice_number}" AS SIGNED))"""
		result = self.session.execute(query)
		for x in result:
			print(f"{x[0]} Record exist for",invoice_number)  
			return x[0]
		
	def updaterow(self,dealer_code,indent_date,status,status_desciption,invoice_number):
		try:
			message=''
			if status=="SUCCESS":
				query=f"""UPDATE `tabInvoice Payment` set status = 'Paid',comment = '{status_desciption}' WHERE 
						  CAST(invoice_number AS SIGNED) = CAST("{invoice_number}" AS SIGNED) 
						  and parent in (SELECT name from tabPayment WHERE payer = '{dealer_code}') and status in ('{Constants.PaymentStatus_PIP.value}','{Constants.PaymentStatus_PPIP.value}')"""
				self.session.execute(query) 

			if status == "FAILURE":    
				query=f"""UPDATE `tabInvoice Payment` set status = 'Failed',comment = '{status_desciption}' WHERE 
						  CAST(invoice_number AS SIGNED) = CAST("{invoice_number}" AS SIGNED)
						  and parent in (SELECT name from tabPayment WHERE payer = '{dealer_code}') and status in ('{Constants.PaymentStatus_PIP.value}','{Constants.PaymentStatus_PPIP.value}')"""
				self.session.execute(query) 
			
			print(f"Invoice Payment with invoice_number {invoice_number} updated")  

			message='Successfull'

		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in updaterow method: ',Except)        
			
		return message
		
	
	def update_finance_entry(self,invoice_number,owner):

		try:
			if not self.session:
				self.session =self.getSession()
			query1= f"""SELECT name,status,document_number  from `tabInvoice Payment` tip WHERE CAST(invoice_number AS SIGNED) = CAST("{invoice_number}" AS SIGNED)"""  
			state = self.session.execute(query1)
			for i in state:
				i=list(i)
				i_status =i[1]
				i_document_number=i[2]
			query= f"""UPDATE `tabFinance Entry` set status = '{i_status}', modified = NOW(), modified_by = '{owner}',owner = '{owner}' WHERE  document_number = '{i_document_number}' and status in ('{Constants.PaymentStatus_PIP.value}','{Constants.PaymentStatus_PPIP.value}')"""    
			print(f"Finance entry with document_number {i_document_number} updated")  

			# query = f"""select * from `tabFinance Entry` tfe WHERE  document_number  in (
			#     SELECT document_number from `tabInvoice Payment` tip WHERE invoice_number = '{invoice_number}')"""
			self.session.execute(query)
			
		except Exception as e:
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in updaterow method: ',Except)              
			

	def update_payment_table(self,invoice_number,dealer_code):
		try:
			if not self.session:
				self.session =self.getSession()
			query =f"""select parent from `tabInvoice Payment` tip WHERE CAST(invoice_number AS SIGNED) = CAST("{invoice_number}" AS SIGNED)
						and parent  in (SELECT name from tabPayment WHERE payer = '{dealer_code}')"""
			set_parent = self.session.execute(query)
			for parent in set_parent:
				parent = str(parent)[1:-2]
				query1 = f"""select status from `tabInvoice Payment` tip where parent = {parent} """
				res_status = self.session.execute(query1)
				list_status=[]
				msg=''
			   
				for status in res_status:
					list_status.append(status)
					extracted_list_status=[]
				 
					for i in list_status:
						x=list(i)
						extracted_list_status.append(x[0])
				if "Failed" in extracted_list_status:
					query = f"""update `tabPayment` set status = "Failed", modified = NOW() WHERE  name = {parent}"""
					self.session.execute(query)
					msg='Failed'
				elif len(set(extracted_list_status))==1:
					
					if extracted_list_status[0]=="Paid":
						query = f"""update `tabPayment` set status = "Paid" , modified = NOW() WHERE  name = {parent}"""
						self.session.execute(query)
						msg='Paid'
						
				elif len(set(extracted_list_status))>1:
						try:
							query = f"""update `tabPayment` set status = "Payment in Progress" ,modified = NOW() WHERE  name = {parent}"""
							self.session.execute(query)
						except Exception as e:
							print(e)
				
			msg='Success'    
		except Exception as e: 
			msg='Failed'
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in updaterow method: ',Except)        
			
		return msg  


	def truncateDataFromDumpTable(self,owner):
		message = ''
		try:
			query = f"""DELETE FROM `tabBank Discounting Dump` where modified_by = "{owner}";""" 

			self.session.execute(query)
			# query = f"UPDATE tabSeries SET current = 0 WHERE name = '{NamingSeries.BCUD.value}'"
			# self.session.execute(query)
			message = "Success"
		except Exception as e:
			message = "Exception"
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in truncateDataFromDumpTable method: ',Except)        
			
		return message


	def processBankdiscountingData(self,file_data):
		try:
			message= 'Failed'
			result = ''
			read = file_data.get('data','')
			owner = file_data.get('user_name','')
			country_name = file_data.get('country','')
			if read != '':
				data = json.loads(read)
				result = self.insertbankdiscountingdump(data,owner,country_name)
				if result == "Success":
					print("Successfully inserted Bank Discounting  data to dump table")
					result = self.update_invoice_payment(owner)
					if result == "Success":
						print("Successfully inserted Bank Discounting data to Invoice Payment")
						self.session.commit()
						result = self.truncateDataFromDumpTable(owner)
						if result == "Success":
							print("Successfully removed Bank discounting Data from dump table")
							self.session.commit()
							message = 'Success'
			else:
				print("No new emails to process.")
		except Exception as e:
			if self.session:
				self.session.rollback()
			Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
			print('Error in processBankCreditUtilisationData method: ',Except)        
			
		finally:
			if self.session:
				self.session.close()
			return message
#endregion


#region bank discounts ICICI

def update_invoice_payment_finance_status(session,data_list,owner):
	try:
		for data in data_list:
			data = data.split('|') #### 0: payer, 1: invoice, 2: status
			status_desc = 'Completed Successfully'
			parent_query = f"""
							select tip.name,tip.parent,tip.document_number  from `tabInvoice Payment` tip
							join tabPayment tp on tp.name = tip.parent 
							WHERE CAST(tip.invoice_number AS SIGNED) = CAST("{data[1]}" AS SIGNED)
							and tip.status in ('{Constants.PaymentStatus_PIP.value}','{Constants.PaymentStatus_PPIP.value}') and tp.payer = '{data[0]}'
						"""
			# print(parent_query)
			res = session.execute(parent_query)
			if res.rowcount > 0:
				for i in res:
					print(i['document_number'])
					inv_query = f""" 
							UPDATE `tabInvoice Payment` set status = "{Constants.Paid_Text.value}",comment = '{status_desc}',modified = now(), modified_by = '{owner}' WHERE name = '{i['name']}'
						"""
					session.execute(inv_query)
					pay_query = f""" 
							UPDATE `tabPayment` set status = "{Constants.Paid_Text.value}", modified = NOW(),  modified_by = '{owner}' WHERE  name = "{i['parent']}"
						"""
					session.execute(pay_query)
					fin_query = f""" 
								UPDATE `tabFinance Entry` set status = '{Constants.Paid_Text.value}' , modified = NOW(), modified_by = '{owner}' WHERE  document_number = '{i['document_number']}' and payer = "{data[0]}" and status in ('{Constants.PaymentStatus_PIP.value}','{Constants.PaymentStatus_PPIP.value}')
							"""
					session.execute(fin_query)
		return "Success"
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in update_invoice_payment_status method: ',Except)   

#endregion
		
#region bank discounts Standard Chartered

def update_invoice_payment_finance_status_scb(session,data,owner):
	try:
		for row in data:
			dealer_code = row['Dealer code']
			invoice_number = row['Customer Ref/Inv No.\n(16 chars)']
			pay_status = Constants.Paid_Text.value    

			status = row['Status']
			if status != Constants.Approved_Text.value:  
				pay_status = Constants.Failed_Text.value

			status_desc = 'Completed Successfully'
			parent_query = f"""
							select tip.name,tip.parent,tip.document_number  from `tabInvoice Payment` tip
							join tabPayment tp on tp.name = tip.parent 
							WHERE CAST(tip.invoice_number AS SIGNED) = CAST("{invoice_number}" AS SIGNED) and tip.status in ('{Constants.PaymentStatus_PIP.value}','{Constants.PaymentStatus_PPIP.value}') and tp.payer = '{dealer_code}'
						"""
			res = session.execute(parent_query)
			if res.rowcount > 0:
				for i in res:
					print(i['document_number'])
					inv_query = f""" 
							UPDATE `tabInvoice Payment` set status = "{pay_status}",comment = '{status_desc}',modified = now(), modified_by = '{owner}' WHERE name = '{i['name']}'
						"""
					session.execute(inv_query)
					pay_query = f""" 
							UPDATE `tabPayment` set status = "{pay_status}", modified = NOW(),  modified_by = '{owner}' WHERE  name = "{i['parent']}"
						"""
					session.execute(pay_query)
					fin_query = f""" 
								UPDATE `tabFinance Entry` set status = '{pay_status}' , modified = NOW(), modified_by = '{owner}' WHERE  document_number = '{i['document_number']}' and payer = "{dealer_code}" and status in ('{Constants.PaymentStatus_PIP.value}','{Constants.PaymentStatus_PPIP.value}')
							"""
					session.execute(fin_query)
		return "Success"
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in update_invoice_payment_status method: ',Except)   

#endregion

#region processed_bank_limit_expiry_calculator

def is_holiday(date, holidays):
	return date in holidays

def holiday_looper(input_date, holiday_dates_list):
	while is_holiday(input_date, holiday_dates_list):
		input_date -= timedelta(days=1)
	return input_date

def bank_limit_holiday_updator(session):
	try:   
		count = 0
		# session = get_session(init_session, jsondata)
		hol_query = f""" select holiday_date from tabHoliday th where year(creation)  = CAST(YEAR(CURRENT_DATE) AS CHAR(6)) order by holiday_date  """
		holiday_dates = session.execute(hol_query).fetchall()
		if holiday_dates != []:
			holiday_dates_list = [date[0] for date in holiday_dates]
			cl_query = """ SELECT name,limit_expiry_date,CAST(YEAR(limit_expiry_date) AS CHAR(6)) AS `limit_expiry_year`,company FROM `tabCredit Limit` tcl where ifnull(processed_limit_expiry_date,'') = '' and CAST(YEAR(limit_expiry_date) AS CHAR(6)) = CAST(YEAR(CURRENT_DATE) AS CHAR(6)); """
			cl_res = session.execute(cl_query)
			for x in cl_res:
				input_date = x['limit_expiry_date']
				cl_name = x['name']
				print("Updating",x['company'])
				count += 1
				processed_limit_expiry_date = holiday_looper(input_date, holiday_dates_list)
				update_query = f""" update `tabCredit Limit` set processed_limit_expiry_date = '{processed_limit_expiry_date}', modified = now() where name = '{cl_name}' """
				session.execute(update_query)
		session.commit()
		print(f"Updated {count} records")
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__} : {e}"
		print("Error in bank_limit_holiday_updator method: ", Ex)
		return str(e)
	
def send_credit_notification_email(email_recp_list,session):
	try:
		mail_subject = ''
		mail_body = ''
		expired_subject = ''
		expired_body = ''
		expiring_subject = ''
		expiring_body = ''
		response = get_email_details()
		if response.status_code == 200:
			email_account = response.json()['message']
			sender_email = email_account["email_id"]
			smtp_server = email_account["smtp_server"]
			smtp_port = email_account["smtp_port"]
			smtp_password = email_account['smtp_password']
			template_query = f"""select name,subject ,response  from `tabEmail Template` tet where name = '{Constants.Credit_Expiry_Email_Master.value}' or name = '{Constants.Credit_Expired_Email_Master.value}'""" 
			res = session.execute(template_query).fetchall()
			if len(res) > 0:
				for e_res in res:
					if e_res['name'] == Constants.Credit_Expiry_Email_Master.value:
						expiring_subject = e_res['subject']
						expiring_body = e_res['response']
					elif e_res['name'] == Constants.Credit_Expired_Email_Master.value:
						expired_subject = e_res['subject']
						expired_body = e_res['response']
				server = smtplib.SMTP(smtp_server, smtp_port)
				server.starttls()
				for recp_dict in email_recp_list:
					if recp_dict['expiry_status'] == 'Expired':
						mail_subject = expired_subject
						mail_body = expired_body
					else:
						mail_subject = expiring_subject
						mail_body = expiring_body
					to_email = recp_dict['user_email']
					# to_email = "digitalfinance.agrica@gmail.com" #### for testing only
					sub = mail_subject.replace('{{expiry_date}}',recp_dict['expiry_date'])
					body = replace_placeholders(mail_body,recp_dict)
					msg = MIMEMultipart()
					msg['To'] = to_email
					# msg['Cc'] = ', '.join(cc_email)
					msg['Subject'] = sub
					msg.attach(MIMEText(body, 'html'))
					server.login(sender_email, smtp_password)
					text = msg.as_string()
					server.sendmail(sender_email, to_email, text)
					print(f"Email sent to {recp_dict['user_email']}")
				server.close()
			else:
				print("Email Template not found")	
		else:
			print(f"Request failed with status code {response.status_code}")
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__} : {e}"
		print("Error in send_credit_notification_email method: ", Ex)
		return str(e)

def get_level_value_for_discounts_email(session, level_res):
	access_level = level_res[0]['parenttype']
	level_value = level_res[0]['level_value']
	res = ''
	query = ''
	if access_level == 'Zone' or access_level == 'Company':
		res = f"'{level_value}'"
	elif access_level == 'Region':
		query = f""" select CONCAT("'", tr.`zone`, "'") as `zone` from tabRegion tr where name = '{level_value}' limit 1"""
	elif access_level == 'Territory':
		query = f""" select CONCAT("'", tr.`zone`, "'") as `zone`  from tabTerritory tt 
					join tabRegion tr on tr.name = tt.region 
					where tt.name = '{level_value}' limit 1"""
	elif access_level == 'User':
		query = f""" SELECT GROUP_CONCAT(CONCAT("'", for_value, "'")) AS `zone`
					FROM `tabUser Permission`
					WHERE `user` = '{level_value}' """
	if query:
		q_res = session.execute(query).fetchall()
		if len(q_res) > 0:
			res = q_res[0]['zone']
	return res

def generate_and_send_discount_scheme_email(session, user_email, email_content, filename, level_res):
	try:
		mail_subject = filename.replace('.pdf', '')
		cc_list = ''
		response = get_email_details()
		if response.status_code == 200:
			email_account = response.json()['message']
			sender_email = email_account["email_id"]
			smtp_server = email_account["smtp_server"]
			smtp_port = email_account["smtp_port"]
			smtp_password = email_account['smtp_password']

			template_query = f"""SELECT name, subject, response FROM `tabEmail Template` tet WHERE name = '{Constants.Discount_Scheme_Email_Template.value}'"""
			res = session.execute(template_query).fetchall()

			# user_email = "digitalfinance.agrica@gmail.com"    #### only for testing
			# cc_list = "digitalfinance.agrica@gmail.com"     #### only for testing

			if len(res) > 0:
				if len(level_res) > 0:
					query = f""" select group_concat(tup.`user`) as 'user' from `tabUser Permission` tup
						join tabUser tu on tup.`user` = tu.name 
						where tup.for_value in ({get_level_value_for_discounts_email(session,level_res)}) and tu.custom_user_type = 'CUT-0005' """

					user_info_res = session.execute(query).fetchall()
					if len(user_info_res) > 0:
						cc_list = user_info_res[0]['user']

				server = smtplib.SMTP(smtp_server, smtp_port)
				server.starttls()

				body = replace_placeholders(res[0]['response'], email_content)

				msg = MIMEMultipart()
				msg['To'] = user_email
				msg['Subject'] = mail_subject

				# Separate To and CC sending
				recipients = [user_email]
				cc_addresses = []
				if cc_list != '' and cc_list != None:
					cc_addresses = cc_list.split(',')
					recipients += cc_addresses
					msg['Cc'] = ', '.join(cc_addresses)

				msg.attach(MIMEText(body, 'html'))
				if filename != '':
					attachment = open(filename, "rb")
					p = MIMEBase('application', 'octet-stream')
					p.set_payload((attachment).read())
					encoders.encode_base64(p)
					p.add_header('Content-Disposition', f"attachment; filename={filename}")
					msg.attach(p)
				text = msg.as_string()
				server.login(sender_email, smtp_password)
				server.sendmail(sender_email, recipients, text)

				print(f"Email sent to {user_email} with CC to {', '.join(cc_addresses)}")
				server.close()
			else:
				print("Email Template not found")
		else:
			print(f"Request failed with status code {response.status_code}")

	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__} : {e}"
		print("Error in send_credit_notification_email method: ", Ex)
		return str(e)



def get_email_details():
	base_url = config['URL']['BASE_URL']
	url = f"{base_url}/api/method/erpnext.agrica.api.get_email_details"
	return requests.get(url)

def replace_placeholders(content, replacement_data):
	for key, value in replacement_data.items():
		placeholder = '{{' + key + '}}'
		content = content.replace(placeholder, value)
	return content

def convert_html_to_pdf_weasyprint(notification_description, html_content,email_content):
	"""
	Converts HTML content with images to a PDF document using WeasyPrint.

	Args:
		html_content (str): The HTML content to convert.
		output_filename (str, optional): The output filename for the PDF. Defaults to "output.pdf".

	Returns:
		str: The path to the generated PDF file on success, None on failure.
	"""

	try:
		# html_suffix = f"The above confirmation has been submitted by {{full_name}} on {{cur_date}} from IP address - {{ip_address}} using Email ID - {{user_email}}, through the digital finance tool."
		html_prefix = f"<h2>{notification_description}</h2>"
		html_suffix = "<br><br><p>The above confirmation has been submitted by {{full_name}} on {{cur_date}} from IP address - {{ip_address}} using Email ID - {{user_email}}, through the digital finance tool.</p>"
		output_filename=f"""Discount Scheme {email_content['cur_date']} {email_content['full_name']}.pdf"""
		page_size = "A4"
		margin = 10  # in millimeters
		html_content += replace_placeholders(html_suffix,email_content)
		html_data = html_prefix + html_content
		# Generate and write PDF
		pdf_filename = output_filename
		HTML(string=html_data).write_pdf(
			pdf_filename,
			stylesheets=[CSS(string=f"""
				@page {{ 
					size: {page_size};
					margin: {margin}mm;
				}}
				img {{
					max-width: 100%;
					height: auto;
					object-fit: contain;
				}}
			""")],
		)
		print(f"PDF successfully generated: {pdf_filename}")
		return pdf_filename
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__} : {e}"
		print("Error in convert_html_to_pdf_weasyprint method: ", Ex)
		return str(e)


# def convert_html_to_pdf(html_content):
# 	try:

# 		pdf_filename = 'output.pdf'
# 		config = pdfkit.configuration(wkhtmltopdf='/path/to/wkhtmltopdf')
# 		pdfkit.from_string(html_content, pdf_filename, configuration=config)
# 		print(f'PDF successfully generated')
# 		return pdf_filename
# 	except Exception as e:
# 		print(f'Error converting HTML to PDF: {str(e)}')

#endregion


#region process_maturity_report_Axis

def process_maturity_report_bank_data(session,file_data,bank_name):
	try:        
		res = 'Failed'
		read = file_data.get('data','')
		owner = file_data.get('user_name','')
		country_name = file_data.get('country','')
		bank_name_list = [Constants.City_bank_name.value,Constants.Brac_bank_name.value]
	
		if read != '':
			# print(read)
			delete_query = f"""delete from `tabMaturity Report Data` where bank_name = '{bank_name}' and payer_code not in {Constants.TestPayerCodes.value}"""
			session.execute(delete_query)
			data = json.loads(read) 
			bulk_insert_string = ""
			if bank_name == Constants.Axis_bank_name.value:
				for row in data:
					payer_code = ''
					invoice_number = row['PARTICULARS']
					account_no = row['ACCOUNT NO']
					dealer_code = row['DEALER CODE']
					comp_name_query = f'''
						select business_area_account,company from `tabCredit Limit` tcl where replace(loan_account,'.0','') = '{account_no}' order by modified desc limit 1
					'''
					comp_name = session.execute(comp_name_query).fetchall()
					if len(comp_name) > 0:
						if dealer_code != '--' and len(dealer_code) > 0:
							payer_code = dealer_code
						else:
							payer_code = comp_name[0]['business_area_account']
						company_name = comp_name[0]['company']
						transaction_date = date_format_converter(str(row['TRANSACTION DATE']),bank_name)
						invoice_date = date_format_converter(str(row['INVOICE DATE']),bank_name)
						transaction_amount = row['TRANSACTION AMOUNT']
						outstanding_amount = row['OUTSTANDING AMOUNT']
						outstanding_days = int(row['OUTSTANDING DAYS'])
						maturity_date = invoice_date if row['MATURITY DATE'] in {'--', ''} else date_format_converter(str(row['MATURITY DATE']), bank_name)
						overdue_amount = row['OVERDUE AMOUNT']
						overdue_days = int(row['OVERDUE DAYS'])
						credit_period = row['CREDIT PERIOD']
						bulk_insert_string += f"""(uuid(), now(), now(), "{owner}", "{owner}", "{company_name}", "{payer_code}", "{account_no}", "{transaction_date}", "{invoice_number}", "{invoice_date}", "{transaction_amount}", "{outstanding_amount}", "{outstanding_days}", "{maturity_date}", "{overdue_amount}", "{overdue_days}", "{credit_period}", "{bank_name}","{country_name}"),"""
			elif bank_name == Constants.ICICI_bank_name.value:
				payer_code = ''
				account_no = ''
				for row in data:	
					invoice_number = ''
					if row['Account No'] != 'Sub-Total':
						trans_p = row['Transaction Particular']
						if payer_code == '':
							payer_code = row['Dealer Code']				
						if account_no == '':
							account_no = row['Account No']
						if 'INF/' in trans_p:
							invoice_number = ((trans_p).split('/'))[2]
						if invoice_number != '':
							inv_date_query = f'''
								select company,posting_date from `tabStatement of Account` tsoa where 
								(
									ext_sys_reference = '{invoice_number}' 
									OR bill_number = '{invoice_number}' 
									OR document_number = '{invoice_number}'
								) 
								OR 
								(
									CAST(ext_sys_reference AS SIGNED) = CAST('{invoice_number}' AS SIGNED)
									OR CAST(bill_number AS SIGNED) = CAST('{invoice_number}' AS SIGNED)
									OR CAST(document_number AS SIGNED) = CAST('{invoice_number}' AS SIGNED)
								) 
								AND payer ='{payer_code}' order by modified desc limit 1
							'''
							inv_date = session.execute(inv_date_query).fetchall()
						else:
							if len(trans_p) > 0 and trans_p != '':
								invoice_number = trans_p
								new_inv_date = date_format_converter(str(row['Tran/Value Date']),bank_name)
								inv_date_query = f'''
									select distinct company,'{new_inv_date}' as posting_date from `tabStatement of Account` tsoa where 
									payer ='{payer_code}' order by modified desc limit 1
								'''
								inv_date = session.execute(inv_date_query).fetchall()
						if len(inv_date) > 0:
							invoice_date = str(inv_date[0]['posting_date'])
							company_name = inv_date[0]['company']
							transaction_date = date_format_converter(str(row['Tran/Value Date']),bank_name)
							if transaction_date == '' or transaction_date == None:
								# print("skipped for \n", row)	
								continue	
							transaction_amount = row['Tran Amount - INR']
							outstanding_amount = row['Balance Outstanding - INR']
							maturity_date = date_format_converter(str(row['Due Date']),bank_name)
							if maturity_date == '' or maturity_date == None:
								continue

							if (row['Overdue Within Cure - INR'] == 0 or row['Overdue Within Cure - INR'] == '0.00') and (row['Overdue BeyondCure - INR'] == 0 or row['Overdue BeyondCure - INR'] == '0.00'):
								overdue_amount = 0
								overdue_days = 0
							elif row['Overdue Within Cure - INR'] != 0 and row['Overdue Within Cure - INR'] != '0.00':
								overdue_amount = row['Overdue Within Cure - INR']
								overdue_days = row['Overdue Within Cure - No Of Days']
							elif row['Overdue BeyondCure - INR'] != 0 and row['Overdue BeyondCure - INR'] != '0.00':
								overdue_amount = row['Overdue BeyondCure - INR']
								overdue_days = row['Overdue Beyond Cure - No Of Days']
							else:
								overdue_amount = 0
								overdue_days = 0

							outstanding_days = (datetime.now() - datetime.strptime(transaction_date,'%Y-%m-%d')).days
							credit_period = 0
							bulk_insert_string += f"""(uuid(), now(), now(), "{owner}", "{owner}", "{company_name}", "{payer_code}", "{account_no}", "{transaction_date}", "{invoice_number}", "{invoice_date}", "{transaction_amount}", "{outstanding_amount}", "{outstanding_days}", "{maturity_date}", "{overdue_amount}", "{overdue_days}", "{credit_period}", "{bank_name}","{country_name}"),"""			
					else:
						payer_code = account_no = ''
			elif bank_name == Constants.SC_bank_name.value:
				for row in data:
					account_no = row['Account No']
					if account_no == 'TOTAL':
						break
					payer_code = ''
					dealer_code = row['Dealer Code']
					comp_name_query = f'''
						select business_area_account,company from `tabCredit Limit` tcl where replace(loan_account,'.0','') = '{account_no}' order by modified desc limit 1
					'''
					comp_name = session.execute(comp_name_query).fetchall()
					if len(comp_name) > 0:
						if dealer_code != '-' and len(dealer_code) > 0:
							payer_code = dealer_code
						else:
							payer_code = comp_name[0]['business_area_account']
						company_name = comp_name[0]['company']
						ageing_value = int(float(row['Aging']))
						transaction_date = row['Effect Date']
						transaction_amount = outstanding_amount = float(row['Outstanding Amount']) if row['Outstanding Amount'] != '-' and row['Outstanding Amount'] != '' else 0
						outstanding_days = ageing_value
						overdue_amount = outstanding_amount if ageing_value > 0 else 0.0
						overdue_days = ageing_value if ageing_value > 0 else 0
						maturity_date = row['Due Date']
						credit_period = ageing_value if ageing_value > 0 else 0

						#### the below fields have some default. This may change in the future
						invoice_date = '2024-12-31'
						invoice_number = '-'

						bulk_insert_string += f"""(uuid(), now(), now(), "{owner}", "{owner}", "{company_name}", "{payer_code}", "{account_no}", "{transaction_date}", "{invoice_number}", "{invoice_date}", "{transaction_amount}", "{outstanding_amount}", "{outstanding_days}", "{maturity_date}", "{overdue_amount}", "{overdue_days}", "{credit_period}", "{bank_name}","{country_name}"),"""
			elif bank_name == 'Standard Chartered Bank Maturity': ##### alternate logic for SCB that uses the first sheet of the maturity report file, remove if not used
				for row in data:
					account_no = row['Account No']
					if account_no == 'TOTAL':
						break
					payer_code = ''
					dealer_code = row['Dealer Code']
					comp_name_query = f'''
						select business_area_account,company from `tabCredit Limit` tcl where replace(loan_account,'.0','') = '{account_no}' order by modified desc limit 1
					'''
					comp_name = session.execute(comp_name_query).fetchall()
					if len(comp_name) > 0:
						if dealer_code != '-' and len(dealer_code) > 0:
							payer_code = dealer_code
						else:
							payer_code = comp_name[0]['business_area_account']
						company_name = comp_name[0]['company']
						transaction_amount = outstanding_amount = float(row['Total Outstanding']) if row['Total Outstanding'] != '-' and row['Total Outstanding'] != '' else 0
						overdue_amount = float(row['Total Overdue']) if row['Total Overdue'] != '-' and row['Total Overdue'] != '' else 0
						overdue_days = int(float(row['Max Overdue Age'])) if row['Max Overdue Age'] != '-' and row['Max Overdue Age'] != '0.0' else 0
						#### the below fields are stored as empty or null. This may change in the future
						credit_period = 0
						outstanding_days = 0
						maturity_date = '2024-12-31'
						transaction_date = '2024-12-31'
						invoice_date = '2024-12-31'
						invoice_number = '-'
						bulk_insert_string += f"""(uuid(), now(), now(), "{owner}", "{owner}", "{company_name}", "{payer_code}", "{account_no}", "{transaction_date}", "{invoice_number}", "{invoice_date}", "{transaction_amount}", "{outstanding_amount}", "{outstanding_days}", "{maturity_date}", "{overdue_amount}", "{overdue_days}", "{credit_period}", "{bank_name}","{country_name}"),"""
			elif bank_name == 'Standard Chartered Bank Discount':    ##### alternate logic for SCB that uses the bank discounts file, remove if not used
				for row in data:
					account_no = row['Dealer Account Number/Code']
					if account_no == 'TOTAL':
						break
					invoice_number = row['Customer Ref/Inv No.\n(16 chars)']
					dealer_code = row['Dealer code']
					if invoice_number != '':
						inv_date_query = f'''
							select company,posting_date from `tabStatement of Account` tsoa where 
							(
								ext_sys_reference = '{invoice_number}' 
								OR bill_number = '{invoice_number}' 
								OR document_number = '{invoice_number}'
							) 
							OR 
							(
								CAST(ext_sys_reference AS SIGNED) = CAST('{invoice_number}' AS SIGNED)
								OR CAST(bill_number AS SIGNED) = CAST('{invoice_number}' AS SIGNED)
								OR CAST(document_number AS SIGNED) = CAST('{invoice_number}' AS SIGNED)
							) 
							AND payer ='{dealer_code}' order by modified desc limit 1
						'''
						inv_date = session.execute(inv_date_query).fetchall()
					if len(inv_date) > 0:
						if len(dealer_code) > 0:
							payer_code = dealer_code
						else:
							payer_code = comp_name[0]['business_area_account']
						company_name = comp_name[0]['company']
						invoice_date = str(inv_date[0]['posting_date'])
						transaction_amount = float(row['Inv amount'])
						outstanding_amount = float(row['Outstanding Amount']) if row['Total Outstanding'] != '' else 0
						outstanding_days = int(row['Outstanding Days'])
						overdue_amount = float(row['Overdue Amount']) if row['Total Overdue'] != '' else 0
						overdue_days = int(row['Ovedue Day'])
						credit_period = ''
						maturity_date = date_format_converter(row['Maturity Date'],bank_name)
						transaction_date = date_format_converter(row['Transaction Date'],bank_name)
						bulk_insert_string += f"""(uuid(), now(), now(), "{owner}", "{owner}", "{company_name}", "{payer_code}", "{account_no}", "{transaction_date}", "{invoice_number}", "{invoice_date}", "{transaction_amount}", "{outstanding_amount}", "{outstanding_days}", "{maturity_date}", "{overdue_amount}", "{overdue_days}", "{credit_period}", "{bank_name}"),"""
			
			elif bank_name in bank_name_list:
				for row in data:
					if row['Transaction Date'] != '' and row['Invoice Date'] != '' and row['Payer Code'] != 'Code not found' and  row['Invoice No']!= '':
						account_no = row['Loan Account No']
						account_no = re.sub('[^A-Za-z0-9]+', '',row['Loan Account No'])
						if account_no.strip() == 'TOTAL':
							break
						payer_code = row['Payer Code']
						comp_name_query = f'''
							select business_area_account,company from `tabCredit Limit` tcl where loan_account = '{account_no}' order by modified desc limit 1
						'''
						comp_name = session.execute(comp_name_query).fetchall()
						if len(comp_name) > 0:
							payer_code = comp_name[0]['business_area_account']
							company_name = comp_name[0]['company']
							country_name = 'Bangladesh'
							transaction_amount = float(row['Transaction Amount'].replace(',', '')) if row['Transaction Amount'] != '-' and row['Transaction Amount'] != '' else 0
							outstanding_amount = float(row['Outstanding Amount'].replace(',', '')) if row['Outstanding Amount'] != '-' and row['Outstanding Amount'] != '' else 0
							overdue_amount = float(row['Overdue Amount'].replace(',', '')) if row['Overdue Amount'] != '-' and row['Overdue Amount'] != '' else 0
		
							transaction_date = date_format_converter(str(row['Transaction Date']),bank_name)
							invoice_date = date_format_converter(str(row['Invoice Date']),bank_name)
							overdue_days = row['Overdue Days']
							invoice_number = row['Invoice No']
							outstanding_days = row['Outstanding Days']
							credit_period = row['Credit Period']
							if row['Maturity Date'] != '':
								maturity_date = date_format_converter(str(row['Maturity Date']),bank_name)
							else:
								maturity_date = '2024-12-31'
						
							bulk_insert_string += f"""(uuid(), now(), now(), "{owner}", "{owner}", "{company_name}", "{payer_code}", "{account_no}", "{transaction_date}", "{invoice_number}", "{invoice_date}", "{transaction_amount}", "{outstanding_amount}", "{outstanding_days}", "{maturity_date}", "{overdue_amount}", "{overdue_days}", "{credit_period}", "{bank_name}","{country_name}"),"""
							
			bulk_insert_string = bulk_insert_string[:-1]
			
			if bulk_insert_string != '':
				insert_query = f'''
								INSERT INTO `tabMaturity Report Data`
								(name, creation, modified, modified_by, owner, company_name, payer_code, account_no, transaction_date, invoice_number, invoice_date, transaction_amount, outstanding_amount, outstanding_days, maturity_date, overdue_amount, overdue_days, credit_period, bank_name, country)
								VALUES {bulk_insert_string}
							'''
				#print(insert_query)
				session.execute(insert_query)
				session.commit()
			else:
				print("No Valid data are present")
			res = 'Success'  
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in process_bank_credit_files method: ',Except)  
		res = 'Failed' 
	return res

def date_format_converter(date_str, bank_name):
	try:
		bank_name_list = list()
		bank_name_list.append(Constants.City_bank_name.value)
		bank_name_list.append(Constants.Brac_bank_name.value)
		if bank_name == Constants.Axis_bank_name.value and date_str != '--' and len(date_str) > 0:
			return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
		if bank_name == Constants.ICICI_bank_name.value and date_str != '--' and len(date_str) > 0:
			try:
				return datetime.strptime(date_str, '%d-%b-%Y').strftime('%Y-%m-%d')
			except ValueError:
				return datetime.strptime(date_str, '%d-%b-%y').strftime('%Y-%m-%d')
		if bank_name == Constants.SC_bank_name.value and len(date_str) > 0:
			try:
				return datetime.strptime(date_str, '%d-%b-%y').strftime('%Y-%m-%d')
			except ValueError:
				return datetime.strptime(date_str, '%d-%b-%Y').strftime('%Y-%m-%d')
		if bank_name in bank_name_list and len(date_str) > 0:
			try:
				return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
			except ValueError:
				return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in date_format_converter method: ', Except)

#endregion


#region credit limit Standard Chartered 

def process_bank_credit_files_sc(session,file_data):
	try:        
		res = 'Failed'
		read = file_data.get('data','')
		owner = file_data.get('user_name','')
		country_name = file_data.get('country','')
		if read != '':
			data = json.loads(read) 
			cf_reset_query = f"update tabCompany set is_channelfinance_enabled = 0, is_channelfinance_enabled_p08 = 0, show_credit_limit = 0 where name in (select company from `tabCredit Limit` tcl where bank_name = '{Constants.SC_bank_name.value}' and business_area_account  not in {Constants.TestPayerCodes.value})"
			session.execute(cf_reset_query)
			for row in data:
				loan_account = re.sub('[^A-Za-z0-9]+', '',row['Account No'])
				if loan_account.strip() == 'TOTAL':
					break
				else:
					business_area_account = row['Dealer Code']
					credit_limit =  float(row['Sanctioned Limits']) if row['Sanctioned Limits'] != '-' and row['Sanctioned Limits'] != '' else 0
					limit_available = float(row['Limit Available']) if row['Limit Available'] != '-' and row['Limit Available'] != '' else 0
					# limit_expiry_date = datetime.strptime(str(row['Dealer Expiry Date']), '%d-%b-%Y')

					limit_expiry_date = "2024-12-31"   ### Hardcoded for now, need more clarification

					utilization = max(credit_limit - limit_available,0)

					bank_total_overdue = float(row['Total Overdue']) if row['Total Overdue'] != '-' and row['Total Overdue'] != '' else 0

					# payment_terms_in_days = int(row['Normal Tenor'])

					payment_terms_in_days = 90   ### 90 is standard for all SC users

					is_shared = 0
					business_area_account = business_area_account.translate(str.maketrans('', '', " '"))
					if business_area_account !='':
						is_shared = 0 if '/' not in business_area_account else 1
						pay_q = f""" select distinct payer_code,external_system from `tabCompany SOH` tcs where payer_code in ({business_area_account.replace('/',',')}) """
						pay_res = session.execute(pay_q)
						if pay_res.rowcount > 0:
							for i in pay_res:
								business_area_account = i['payer_code']
								ext_sys = i['external_system']
								res = credit_data_processor_sc(session,business_area_account,loan_account,credit_limit,limit_expiry_date,utilization,bank_total_overdue,is_shared,payment_terms_in_days,owner,ext_sys,country_name)                     
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in process_bank_credit_files method: ',Except)  
		res = 'Failed' 
	session.commit()
	return res

def credit_data_processor_sc(session,business_area_account,loan_account,credit_limit,limit_expiry_date,utilization,bank_total_overdue,is_shared,payment_terms_in_days,owner,ext_sys,country_name):
	try:
		response = 'Success'
		# admin = Constants.AdminUser.value
		supplier_type = Constants.Supplier_Type.value
		supplier = 'Bayer'
		supplier_type_desc = 'Bank'
		bank_name = Constants.SC_bank_name.value

		#### only PBC companies are considered for now, logic might have to be changed in the future
		company_query = f""" select distinct parent from `tabCompany SOH` tcs where payer_code = '{business_area_account}' AND external_system = '{ext_sys}' limit 1 """
		company_res = (session.execute(company_query).fetchall())

		if len(company_res) > 0:
			company = company_res[0]['parent']
			check_query = f""" select name from `tabCredit Limit` tcl where business_area_account ='{business_area_account}' """
			check_res = session.execute(check_query).fetchall()
			lm_str = ''
			if len(check_res) > 0:
				print(f"Updating for {business_area_account} | {loan_account} | {credit_limit} | {utilization} | {company} ")

				##### remove later, unnecessary
				lim_check_query = f"""select name from `tabCredit Limit` tcl WHERE name='{check_res[0]['name']}' and ifnull(limit_expiry_date,'') = '';"""
				lim_check_res = session.execute(lim_check_query).fetchall()
				if len(lim_check_res) != 0:
					lm_str = f"limit_expiry_date='{limit_expiry_date}',"

				update_query = f"""UPDATE `tabCredit Limit`
								SET modified=now(), loan_account='{loan_account}', credit_limit='{credit_limit}', bank_total_overdue='{bank_total_overdue}',last_updated_date_from_sap=now(), utilization='{utilization}', {lm_str} processed_limit_expiry_date = NULL, enabled=1, is_channel_finance=1, is_shared='{is_shared}', bank_name='{bank_name}' WHERE name='{check_res[0]['name']}';"""  
				session.execute(update_query)
			else:
				cl_name = str(uuid.uuid4().hex) 
				insert_query = f"""INSERT INTO `tabCredit Limit`
									(name, creation, modified, modified_by, owner, naming_series, company, supplier_type, business_area_account, supplier, supplier_type_desc, loan_account, credit_limit, bank_total_overdue, last_updated_date_from_sap, status, utilization, limit_expiry_date, enabled, is_channel_finance, is_shared, bank_name,external_system, business_area_desc, country)
									VALUES('{cl_name}', now(), now(), '{owner}', '{owner}', 'UUID', '{company}', '{supplier_type}','{business_area_account}','{supplier}', '{supplier_type_desc}', '{loan_account}', '{credit_limit}','{bank_total_overdue}', now(), 'Active', '{utilization}', '{limit_expiry_date}', 1, 1, '{is_shared}', '{bank_name}','{ext_sys}',(select description from `tabExternal System` where name = "{ext_sys}"), "{country_name}");"""
				# print(insert_query)
				session.execute(insert_query) 
			update_company_cf(session,company,payment_terms_in_days,owner,ext_sys)
		else:
			print(f"Company with {business_area_account} does not exist in the system.")
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in credit_data_processor_sc method: ',Except)
		response = 'Failed'
	return response

#region city & brac 
def process_bank_credit_files_Common(session,file_data,bank_name,country_name,owner):
	try:        
		res = 'Failed'
		# read = file_data.get('data','')
		# owner = file_data.get('user_name','')
		# country_name = file_data.get('country','')
		ext_sys = BusinessArea.P4S.value
		if file_data != '':
			data = json.loads(file_data)			
			cf_reset_query = f"update tabCompany set is_channelfinance_enabled_p4s = 1, show_credit_limit = 1 where name in (select company from `tabCredit Limit` tcl where bank_name = '{bank_name}' and business_area_account  not in {Constants.TestPayerCodes.value})"
			session.execute(cf_reset_query)
			for row in data:
				loan_account = re.sub('[^A-Za-z0-9]+', '',row['Loan Account No']) 
				if loan_account.strip() == 'TOTAL':
					break
				else:
					business_area_account = row['Payer Code']
					credit_limit =  float(row['Sanction Limit']) if row['Sanction Limit'] != '-' and row['Sanction Limit'] != '' else 0
					utilization = float(row['Limit Utilized'])
					overdue_days = int(row['Overdue Days']) if row['Overdue Days'] != '-' else 0
					bank_total_overdue = float(row['Total Overdue Amount']) if row['Total Overdue Amount'] != '-' and row['Total Overdue Amount'] != '' else 0
					limit_expiry_date =  row['Limit Expiry Date']  

					is_shared = 0
					if business_area_account != '':
						res = credit_data_processor_Common(session,business_area_account,loan_account,credit_limit,limit_expiry_date,utilization,bank_total_overdue,owner,ext_sys,overdue_days,is_shared,country_name,bank_name)
					else:
						is_shared = 1
						pay_q = f""" select distinct payer_code,external_system from `tabCompany SOH` tcs where payer_code in ({business_area_account.replace('/',',')}) """
						pay_res = session.execute(pay_q)
						if pay_res.rowcount > 0:
							for i in pay_res:
								business_area_account = i['payer_code']
								ext_sys = i['external_system']
								res = credit_data_processor_Common(session,business_area_account,loan_account,credit_limit,limit_expiry_date,utilization,bank_total_overdue,owner,ext_sys,overdue_days,is_shared,country_name,bank_name)
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in process_bank_credit_files  common method: ',Except)  
		res = 'Failed' 
	session.commit()
	return res


def credit_data_processor_Common(session,business_area_account,loan_account,credit_limit,limit_expiry_date,utilization,bank_total_overdue,owner,ext_sys,overdue_days,is_shared,country_name,bank_name):
	try:
		if bank_name == 'BRAC':
			payment_terms_in_days = 90
		elif bank_name == 'CITY':
			payment_terms_in_days = 45
   
		response = 'Success'
		# admin = Constants.AdminUser.value
		supplier_type = Constants.Supplier_Type.value
		supplier = 'Bayer'
		supplier_type_desc = 'Bank'
		status = 'Active'
		
		company_query = f""" select distinct parent from `tabCompany SOH` tcs where payer_code = '{business_area_account}' AND external_system = '{ext_sys}' limit 1 """
		company_res = (session.execute(company_query).fetchall())

		if len(company_res) > 0:
			company = company_res[0]['parent']
			check_query = f""" select name from `tabCredit Limit` tcl where business_area_account ='{business_area_account}' """
			check_res = session.execute(check_query).fetchall()
			lm_str = ''
			if len(check_res) > 0:
				print(f"Updating for {business_area_account} | {loan_account} | {credit_limit} | {utilization} | {company} ")
				update_query = f"""UPDATE `tabCredit Limit`
								SET modified=now(), loan_account='{loan_account}', credit_limit={credit_limit}, bank_total_overdue={bank_total_overdue},last_updated_date_from_sap=now(), utilization={utilization}, {lm_str} processed_limit_expiry_date = NULL, enabled=1, is_channel_finance=1, is_shared='{is_shared}', bank_name='{bank_name}',overdue_days={overdue_days},country='{country_name}',limit_expiry_date='{limit_expiry_date}',status='{status}' WHERE name='{check_res[0][0]}';""" 

				session.execute(update_query)
			else:
				cl_name = str(uuid.uuid4().hex) 
				insert_query = f"""INSERT INTO `tabCredit Limit`
									(name, creation, modified, modified_by, owner, naming_series, company, supplier_type, business_area_account, supplier, supplier_type_desc, loan_account, credit_limit, bank_total_overdue, last_updated_date_from_sap, status, utilization, limit_expiry_date, enabled, is_channel_finance, is_shared, bank_name,external_system, business_area_desc,overdue_days,country)
									VALUES('{cl_name}', now(), now(), '{owner}', '{owner}', 'UUID', '{company}', '{supplier_type}','{business_area_account}','{supplier}', '{supplier_type_desc}', '{loan_account}', '{credit_limit}','{bank_total_overdue}', now(), '{status}', '{utilization}', '{limit_expiry_date}', 1, 1, '{is_shared}', '{bank_name}','{ext_sys}',(select description from `tabExternal System` where name = "{ext_sys}"),{overdue_days},'{country_name}');"""
			
				session.execute(insert_query) 
			update_company_cf(session,company,payment_terms_in_days,owner,ext_sys)
		else:
			print(f"Company with {business_area_account} does not exist in the system.")
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in credit_data_processor_common method: ',Except)
		response = 'Failed'
	return response	

#endregion
