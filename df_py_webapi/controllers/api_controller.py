from email import message
import json
import os
from fastapi.responses import JSONResponse
from commons.enum import Constants, UserAlertConstant
from commons.utils import no_format
from config.config import config, logging
from commons.mariadb import SessionMDB
import sys
import base64
from commons.http_operations import post, get
from datetime import datetime, timedelta
import requests
from config.df_security import DFSecurity


sys.path.append("..")
basePath = config['URL']['BASE_URL']
client_id = config['AZURE_KEYS']['CLIENT_ID']
tenant_id = config['AZURE_KEYS']['TENANT_ID']
client_secret = config['AZURE_KEYS']['CLIENT_SECRET']


from commons.services import (BankCreditUtilisation, BankDiscounting, bank_limit_holiday_updator, convert_html_to_pdf_weasyprint, file_status_updator, generate_and_send_discount_scheme_email, process_bank_credit_files, process_bank_credit_files_sc, send_credit_notification_email, update_invoice_payment_finance_status,notification_status_updator, process_maturity_report_bank_data, update_invoice_payment_finance_status_scb,process_bank_credit_files_Common)

class WebAPIController():

	def __init__(self):
		pass
	
	@classmethod
	async def code_and_refresh_token_generator(self, **content):
		try:
			message = response = ''
			statusCode = 200
			session = SessionMDB()
			request_type = content.get('request_type', '')
			code = content.get('code', '')
			scope = "User.Read Mail.ReadWrite Mail.Send offline_access"
			redirect_uri = "http://localhost/myapp"

			if request_type == 'Code':
				url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
				response_type = "code"
				response_mode = "query"
				state = "12345"

				# Create the query parameters for the URL
				params = {
					"client_id": client_id,
					"response_type": response_type,
					"redirect_uri": redirect_uri,
					"response_mode": response_mode,
					"scope": scope,
					"state": state
				}

				# Make the GET request to the authorization endpoint
				response = requests.get(url, params=params)

				# Check if the request was successful
				if response.status_code == 200:
					print("Authorization request successful.")
					print("Redirect URL:", response.url)
					message = response = "Redirect URL:" + response.url
				else:
					print("Error:", response.text)
					message = response = response.text

			elif request_type == 'Token' and code != '':
				url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
				grant_type = "authorization_code"

				# Create the data payload for the POST request
				data = {
					"client_id": client_id,
					"scope": scope,
					"code": code,
					"redirect_uri": redirect_uri,
					"grant_type": grant_type,
					"client_secret": client_secret
				}

				# Make the POST request
				response = requests.post(url, data=data)

				# Check if the request was successful
				if response.status_code == 200:
					# Parse the response and extract the access token
					# print(response.json())
					# access_token = response.json().get("access_token")
					refresh_token = response.json().get("refresh_token")
					up_query = f"""
						update tabSingles set value = '{refresh_token}' where doctype = 'azure_key' and field = 'refresh_token'
					"""
					session.execute(up_query)
					session.commit()
					message = response = "Successfully Updated Refresh Token"
					statusCode = 200
				else:
					print("Error:", response.text)
			else:
				message = response = "Code is empty"
		except Exception as e:
			statusCode = 500
			message = "Error"
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print("Error in code_and_refresh_token_generator method: ", Ex)         
		finally:
			# return response
			return JSONResponse(status_code=statusCode, content= DFSecurity().encryptDataWithResponse(status_code=statusCode, message=message, content_to_encrypt=response) )
		
	@classmethod
	async def process_payment_terms(self,**file_data):
		try:
			print("processing")
			statusCode = 500
			session = SessionMDB()
			att_status = 'Failed'
			count = 0
			response = ''
			comments = ''
			# is_channelfinance_enabled = 1
			payer_code = ''
			payment_terms_in_days = ''
			customer_accepted_autopulling = ''
			read = file_data.get('data','')
			owner = file_data.get('user_name','')
			filename = file_data.get('file_name','')
			# print(read,owner)
			if read != '':
				data = json.loads(read)
				for row in data:
					count += 1
					payer_code = row['Customer Code']
					payment_terms_in_days = row['Payment Terms Days']
					customer_accepted_autopulling = row['Auto pull days for ERP_Lite']

					up_query = f"""
						UPDATE
							tabCompany tc
						JOIN
							`tabCompany SOH` tsoh ON tsoh.parent = tc.name
						SET
							payment_terms_in_days = '{payment_terms_in_days}',
							customer_accepted_autopulling = '{customer_accepted_autopulling}',
							tc.modified_by = '{owner}',
							tc.modified = NOW()
						WHERE tsoh.payer_code = '{payer_code}'
					"""
					# '''is_channelfinance_enabled = '{is_channelfinance_enabled}',''' maybe added back 
					session.execute(up_query)
					
				response = f"Successfully updated {count} companies."
				att_status = "Success"
			statusCode = 200
			message = f"Successfully updated {count} companies."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_payment_terms:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response
	
	@classmethod
	async def process_bank_credit_ICICI(self,**file_data):
		try:
			print("processing")
			statusCode = 500
			session = SessionMDB()
			att_status = 'Failed'
			count = 0
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			att_status = process_bank_credit_files(session,file_data)
			if att_status == 'Success':
				bank_limit_holiday_updator(session)
				# await self.credit_notification_processor()
				statusCode = 200
				message = f"Successfully updated {count} companies."
			else:
				statusCode = 500
				att_status = 'Failed'
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_bank_credit_ICICI:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response
	
	@classmethod 
	async def process_bank_credit_Axis(self,**file_data):
		try:
			print("processing")
			session = SessionMDB()
			statusCode = 500
			att_status = 'Failed'
			count = 0
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			bank_credit_utilisation = BankCreditUtilisation()
			att_status = bank_credit_utilisation.processBankCreditUtilisationData(file_data)
			if att_status == 'Success':
				bank_limit_holiday_updator(session)
				# await self.credit_notification_processor()
				statusCode = 200
				message = f"Successfully updated {count} companies."
			else:
				att_status = 'Failed'
				statusCode = 500
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_bank_credit_Axis:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response
	
	@classmethod 
	async def process_bank_discount_Axis(self,**file_data):
		try:
			print("processing")
			statusCode = 500
			att_status = 'Failed'
			count = 0
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			bank_credit_utilisation = BankDiscounting()
			att_status = bank_credit_utilisation.processBankdiscountingData(file_data)
			if att_status == 'Success':
				statusCode = 200
				message = f"Successfully updated {count} companies."
			else:
				att_status = 'Failed'
				statusCode = 500
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_bank_discount_Axis:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		session = SessionMDB()
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response

	@classmethod 
	async def process_bank_discount_ICICI(self,**file_data):
		try:
			session = SessionMDB()
			print("processing")
			statusCode = 500
			att_status = 'Failed'
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			bank_name = file_data.get('bank_name','')
			read = file_data.get('data','')
			owner = file_data.get('onwer','')
			data_list = []
			payer_code = ''
			if read != '':
				data = json.loads(read)
				for row in data:
					if row['Account No'] != 'Sub-Total':
						if payer_code == '':
							payer_code = row['Dealer Code']
						if 'INF/' in row['Transaction Particular']:
							invoice_no = ((row['Transaction Particular']).split('/'))[2]
							status = 'Paid'
							data_list.append(f"{payer_code}|{invoice_no}|{status}")
					else:
						payer_code = ''
				# print(data_list)
				if data_list:
					att_status = update_invoice_payment_finance_status(session,data_list,owner)
			if att_status == 'Success':
				message = "Successfully updated payment entries."
				att_status = process_maturity_report_bank_data(session,file_data,bank_name)
				if att_status == 'Success':
					await self.maturity_report_notification_processor(bank_name)
					statusCode = 200
					print(f"Successfully updated Maturity Report Data for {bank_name} bank.")
					statusCode = 200
					message = "Successfully updated payment entries and maturity report"
				else:
					att_status = 'Failed'
					statusCode = 500
					message = "Failed, please check the uploaded file."
			else:
				att_status = 'Failed'
				statusCode = 500
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_bank_discount_Axis:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response
	
	@classmethod 
	async def reset_tax_dec_response(self,**file_data):
		try:
			session = SessionMDB()
			print("processing")
			statusCode = 500
			att_status = 'Failed'
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			data = file_data.get('data','')
			owner = file_data.get('owner','')
			str_read = data.replace('[','(').replace(']',')')
			query = f"""
					update `tabCustomer Alert` set has_responded = 0,modified = now(), modified_by = '{owner}' where company in (
					select name from tabCompany tc where pan not in {str_read})
					and alert_name = (select name from `tabUser Alert` tua where alert_type = '{Constants.Tax_Declaration.value}' order by creation desc limit 1)
			"""
			session.execute(query)
			att_status = 'Success'
			statusCode = 200
			message = "Successfully updated Customer Alerts."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in reset_tax_dec_response:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response
	
	@classmethod 
	async def user_notification_data_processor(self,**alert_data):
		try:
			session = SessionMDB()
			print("Processing")
			statusCode = 500
			att_status = 'Failed'
			user_query = ''
			response = ''
			recipient_list = []
			owner = alert_data.get('user_name','')
			notice_name = alert_data.get('notice_name','')
			notice_subject = alert_data.get('notice_subject','')
			all_sales_users = alert_data.get('all_sales_users',0)
			all_distributor_users = alert_data.get('all_distributor_users',0)
			all_credit_managers = alert_data.get('all_credit_managers',0)
			all_head_office_users = alert_data.get('all_head_office_users',0)
			recp_data_list = alert_data.get('recp_data_list','')
			noti_type = "Manual"

			exec_str = notification_status_updator(notice_name,"Processing")
			session.commit()
			user_type_filter = '('
			if all_distributor_users:
				user_type_filter += '''"CUT-0001",''' # only distributor
			if all_sales_users:
				user_type_filter += '''"CUT-0002","CUT-0003","CUT-0006",''' # sales users (TBM, CM, CU Lead)
			if all_credit_managers:
				user_type_filter += '''"CUT-0005",''' # CM
			if all_head_office_users:
				user_type_filter += '''"CUT-0007",''' # HO
			user_type_filter = user_type_filter[:-1] + ')' 
			if recp_data_list != []:
				for i in recp_data_list:
					if i['level'] == 'User':
						user_query = f"""
							select distinct tup.`user`,tup.for_value,tu.custom_user_type,'{i['level']}' as `level`,'{i['level_value']}' as `level_value` from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user`
							where tu.name = '{i['level_value']}'
							group by tup.`user`,tu.custom_user_type 
						"""
					elif i['level'] == 'Company':
						user_query = f"""
							select distinct tup.`user`,tup.for_value,tu.custom_user_type,'{i['level']}' as `level`,'{i['level_value']}' as `level_value` from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user`
							where for_value = '{i['level_value']}'
							and tu.custom_user_type in {user_type_filter}
							group by tup.`user`,tu.custom_user_type
						"""
					elif i['level'] == 'Zone':
						user_query = f"""
							select distinct tup.`user`,tup.for_value,tu.custom_user_type,'{i['level']}' as `level`,'{i['level_value']}' as `level_value` from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user` 
							left join (select distinct parent,territory from `tabCompany SOH`) tcs on tcs.parent = tup.for_value
							left join tabTerritory tt on tt.name = tcs.territory
							left join tabRegion tr on tr.name = tt.region
							where tr.zone = '{i['level_value']}'
							and tu.custom_user_type in {user_type_filter}
							group by tup.`user`,tu.custom_user_type
						"""
					elif i['level'] == 'Region':
						user_query = f"""
							select distinct tup.`user`,tup.for_value,tu.custom_user_type,'{i['level']}' as `level`,'{i['level_value']}' as `level_value` from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user` 
							left join (select distinct parent,territory from `tabCompany SOH`) tcs on tcs.parent = tup.for_value
							left join tabTerritory tt on tt.name = tcs.territory
							where tt.region = '{i['level_value']}'
							and tu.custom_user_type in {user_type_filter} 
							group by tup.`user`,tu.custom_user_type
						"""
					elif i['level'] == 'Territory':
						user_query = f"""
							select distinct tup.`user`,tup.for_value,tu.custom_user_type,'{i['level']}' as `level`,'{i['level_value']}' as `level_value`  from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user` 
							left join (select distinct parent,territory from `tabCompany SOH`) tcs on tcs.parent = tup.for_value
							where tcs.territory = '{i['level_value']}'
							and tu.custom_user_type in {user_type_filter} 
							group by tup.`user`,tu.custom_user_type
						"""
					elif i['level'] == 'Country':
						user_query = f"""
							select distinct tup.`user`,tup.for_value,tu.custom_user_type,'{i['level']}' as `level`,'{i['level_value']}' as `level_value`  from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user` 
							left join (select distinct parent,territory,country from `tabCompany SOH`) tcs on tcs.parent = tup.for_value
							where tcs.country = '{i['level_value']}'
							and tu.custom_user_type in {user_type_filter} 
							group by tup.`user`,tu.custom_user_type
						"""
					# print(user_query)
					user_res = session.execute(user_query).fetchall()
					recipient_list += user_res
				
			bulk_insert_string = ''
			if recipient_list:
				for recp in recipient_list:
					# print("recp",recp)
					bulk_insert_string += f"""(uuid(), now(), now(), "{owner}", "{owner}", "{notice_name}", "{recp['level']}", "{recp['user']}", "{notice_subject}", "{recp['level_value']}", 0, NULL,"{noti_type}"),"""
					# not_in_query += f""
				bulk_insert_string = bulk_insert_string[:-1]
			# print(bulk_insert_string)
			bulk_insert_query = f"""
				INSERT INTO `tabUser Notifications`
				(name, creation, modified, modified_by, owner, notification_id,  parenttype, user_email, notification_description, level_value, has_read, read_on, notification_type)
				VALUES {bulk_insert_string}
			"""
			session.execute(bulk_insert_query)
			att_status = 'Success'
			statusCode = 200
			message = f"Successfully updated Customer Alerts."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in user_notification_data_processor:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			message = "Failed, please check the uploaded file."
		exec_str = notification_status_updator(notice_name,att_status)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response

	@classmethod 
	async def credit_notification_processor(self):
		try:
			session = SessionMDB()
			print("credit_notification_processor Processing")
			statusCode = 500
			att_status = 'Failed'
			user_query = ''
			response = ''
			recipient_list = []
			owner = "Administrator"
			notice_name = ""
			notice_subject = ""
			expiring_notice_name = ""
			expiring_notice_subject = ""
			expired_notice_name = ""
			expired_notice_subject = ""
			noti_type = "Auto"
			del_user_list = []
			email_recp_list = []
			message = ''
			master_noti_info = f"""select name,notice_subject from `tabNotification Alert` tna where name = '{Constants.Credit_Expiry_Notification_Master.value}' or name = '{Constants.Credit_Expired_Notification_Master.value}' """
			master_noti_res = session.execute(master_noti_info).fetchall()
			if len(master_noti_res) > 0:
				for master_res in master_noti_res: 
					if master_res['name'] == Constants.Credit_Expiry_Notification_Master.value:
						expiring_notice_name = master_res['name']
						expiring_notice_subject = master_res['notice_subject']
					elif master_res['name'] == Constants.Credit_Expired_Notification_Master.value:
						expired_notice_name = master_res['name']
						expired_notice_subject = master_res['notice_subject']
				recp_data_query = """
					SELECT DISTINCT
						company,
						ifnull(processed_limit_expiry_date,limit_expiry_date) as `expiry_date`, CURDATE(), 
						CASE
							WHEN DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) < 0 THEN 'Expired'
							WHEN DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 7 THEN '1 week away'
							WHEN DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 14 THEN '2 weeks away'
							WHEN DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 21 THEN '3 weeks away'
							WHEN (DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 30 or DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 31) THEN 'Month away'
							WHEN (DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 60 OR DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 61 OR DATEDIFF(IFNULL(processed_limit_expiry_date, limit_expiry_date), CURDATE()) = 62) THEN '2 Months away'
						END AS expiry_status, bank_name
					FROM
					`tabCredit Limit` tcl
					LEFT JOIN tabCompany tc on tc.name = tcl.company 
					WHERE
					(IFNULL(processed_limit_expiry_date, limit_expiry_date) = DATE_ADD(CURDATE(), INTERVAL 1 WEEK) OR
					IFNULL(processed_limit_expiry_date, limit_expiry_date) = DATE_ADD(CURDATE(), INTERVAL 2 WEEK) OR
					IFNULL(processed_limit_expiry_date, limit_expiry_date) = DATE_ADD(CURDATE(), INTERVAL 3 WEEK) OR
					IFNULL(processed_limit_expiry_date, limit_expiry_date) = DATE_ADD(CURDATE(), INTERVAL 1 MONTH) OR
					IFNULL(processed_limit_expiry_date, limit_expiry_date) = DATE_ADD(CURDATE(), INTERVAL 2 MONTH) OR 
					IFNULL(processed_limit_expiry_date, limit_expiry_date) < CURDATE())
					AND lower(status) = 'active' AND tc.is_enabled = 1
					ORDER BY expiry_date;
				"""
				# print(recp_data_query)
				recp_data_list = session.execute(recp_data_query).fetchall()
				if len(recp_data_list) > 0:
					repeat_del_query = f"""delete from `tabUser Notifications` where date(creation) = CURDATE() and (notification_id = '{Constants.Credit_Expiry_Notification_Master.value}' or notification_id = '{Constants.Credit_Expired_Notification_Master.value}') """
					session.execute(repeat_del_query)
					session.commit()
					for i in recp_data_list:
						user_query = f"""
							select distinct tup.`user`, tu.full_name as `full_name`, tup.for_value,tu.custom_user_type,'Company' as `level`,'{i['company']}' as `level_value`,
							DATE_FORMAT('{i['expiry_date']}', '%D %b %Y') AS 'expiry_date', '{i['expiry_status']}' AS expiry_status,
							CURDATE() as 'start_date', DATE_ADD(CURDATE(), INTERVAL 2 MONTH) as 'end_date', tc.name_for_print, '{i['bank_name']}' as `bank_name` from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user`
							left join tabCompany tc on tc.name = tup.for_value
							where for_value = '{i['company']}'
							and tu.custom_user_type = "CUT-0001"
							group by tup.`user`,tu.custom_user_type
						"""
						# print(user_query)
						user_res = session.execute(user_query).fetchall()
						recipient_list += user_res
					bulk_insert_string = ''
					if recipient_list:
						for recp in recipient_list:
							if recp['expiry_status'] == 'Expired':
								notice_name = expired_notice_name
								notice_subject = expired_notice_subject
							else:
								notice_name = expiring_notice_name
								notice_subject = expiring_notice_subject
							user_data_dict = {   
								"user_email": recp['user'],
								"expiry_date": recp['expiry_date'],
								"full_name": recp['full_name'],
								"company": recp['name_for_print'],
								"expiry_status": recp['expiry_status'],
								"bank_name": recp['bank_name']
							} #### also content_modification_value
							updated_notic_subject = notice_subject.replace("{{expiry_date}}",recp['expiry_date']).replace("{{company}}",recp['name_for_print'])
							exp_check_query = f"""select * from `tabUser Notifications` tun where notification_id = '{notice_name}' and notification_description = '{updated_notic_subject}'and creation <= DATE_ADD(creation, INTERVAL 2 MONTH)"""
							exp_check_res = session.execute(exp_check_query)
							if exp_check_res.rowcount <= 0:
								user_data_string = json.dumps(user_data_dict)
								bulk_insert_string += f"""(uuid(), now(), now(), '{owner}', '{owner}', '{notice_name}', '{recp['level']}', '{recp['user']}', '{updated_notic_subject}', '{recp['level_value']}', 0, NULL, '{noti_type}', '{recp['start_date']}', '{recp['end_date']}', '{user_data_string}'),"""
								del_user_list.append(recp['user'])
								email_recp_list.append(user_data_dict)
						bulk_insert_string = bulk_insert_string[:-1]
					# print(bulk_insert_string)
					if del_user_list != []:
						del_user_tuple = "(" + ", ".join(['"' + item + '"' for item in del_user_list]) + ")"
						old_noti_del_query = f""" delete from `tabUser Notifications` where user_email in {del_user_tuple} and creation <= DATE_SUB(NOW(), INTERVAL 2 MONTH) and notification_type = "Auto" and (notification_id = '{Constants.Credit_Expiry_Notification_Master.value}' or notification_id = '{Constants.Credit_Expired_Notification_Master.value}')"""
						session.execute(old_noti_del_query)
						session.commit()
					if bulk_insert_string != '':	
						bulk_insert_query = f"""
							INSERT INTO `tabUser Notifications`
							(name, creation, modified, modified_by, owner, notification_id,  parenttype, user_email, notification_description, level_value, has_read, read_on, notification_type, start_date, end_date, content_modification_value)
							VALUES {bulk_insert_string}
						"""
						# print(bulk_insert_query)
						session.execute(bulk_insert_query)
						session.commit()
					else:
						print("No new notifications to insert")
					# print(email_recp_list)
					if email_recp_list != []:
						send_credit_notification_email(email_recp_list,session)
					else:
						print("No emails to send")
					att_status = 'Success'
					statusCode = 200
					message = f"Successfully Aded Credit Limit Notification Alerts."
			else:
				att_status = 'Failed'
				statusCode = 500
				message = "A master notification template is not present."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in credit_notification_processor:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			message = "Failed to send auto notifications."
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		print(f"credit_notification_processor {att_status}")
		return response

	@classmethod 
	async def process_maturity_report_data(self,**file_data):
		try:
			print("process_maturity_report_data processing")
			session = SessionMDB()
			statusCode = 500
			att_status = 'Failed'
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			bank_name = file_data.get('bank_name','')
			att_status = process_maturity_report_bank_data(session,file_data,bank_name)
			if att_status == 'Success':
				await self.maturity_report_notification_processor(bank_name)
				statusCode = 200
				message = f"Successfully updated Maturity Report Data for {bank_name} bank."
			else:
				att_status = 'Failed'
				statusCode = 500
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_maturity_report_data:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(f"process_maturity_report_data {att_status}")
		return response
	
	@classmethod
	async def maturity_report_notification_processor(self, bank_name):
		try:
			session = SessionMDB()
			print("maturity_report_notification_processor Processing")
			user_query = ''
			response = ''
			recipient_list = []
			owner = "Administrator"
			notice_name = ""
			notice_subject = ""
			noti_type = "Auto"
			del_user_list = []
			email_recp_list = []
			master_noti_info = f"""select name,notice_subject from `tabNotification Alert` tna where name = '{Constants.Maturity_Report_Notification_Master.value}'"""
			master_noti_res = session.execute(master_noti_info).fetchall()[0]
			if len(master_noti_res) > 0:
				notice_name = master_noti_res['name']
				notice_subject = master_noti_res['notice_subject']
				recp_data_query = f"""
					SELECT DISTINCT
						company_name ,payer_code ,maturity_date ,bank_name, account_no ,invoice_number,
						CASE
							WHEN DATEDIFF(maturity_date, CURDATE()) = 7 THEN '1 week away'
							WHEN DATEDIFF(maturity_date, CURDATE()) = 14 THEN '2 weeks away'
							WHEN DATEDIFF(maturity_date, CURDATE()) = 21 THEN '3 weeks away'
							WHEN (DATEDIFF(maturity_date, CURDATE()) = 30 OR DATEDIFF(maturity_date, CURDATE()) = 31) THEN 'Month away'
							WHEN (DATEDIFF(maturity_date, CURDATE()) = 60 OR DATEDIFF(maturity_date, CURDATE()) = 61 OR DATEDIFF(maturity_date, CURDATE()) = 62) THEN '2 Months away'
						END AS maturity_status
					FROM
					`tabMaturity Report Data`
					WHERE bank_name = '{bank_name}' and
					(maturity_date = DATE_ADD(CURDATE(), INTERVAL 1 WEEK) OR
					maturity_date = DATE_ADD(CURDATE(), INTERVAL 2 WEEK) OR
					maturity_date = DATE_ADD(CURDATE(), INTERVAL 3 WEEK) OR
					maturity_date = DATE_ADD(CURDATE(), INTERVAL 1 MONTH) OR
					maturity_date = DATE_ADD(CURDATE(), INTERVAL 2 MONTH))
					ORDER BY maturity_date;
				""" ######### gets all the records where maturity date is 10 days from current date
				recp_data_list = session.execute(recp_data_query).fetchall()
				if len(recp_data_list) > 0:
					repeat_del_query = f"""delete from `tabUser Notifications` where date(creation) = CURDATE() and notification_id = '{Constants.Maturity_Report_Notification_Master.value}'"""
					session.execute(repeat_del_query)
					session.commit()
					for i in recp_data_list:
						user_query = f"""
							select distinct tup.`user`, tu.full_name as `full_name`, tup.for_value,tu.custom_user_type,'Company' as `level`,'{i['company_name']}' as `level_value`, '{i['payer_code']}' as `payer_code`,
							DATE_FORMAT('{i['maturity_date']}', '%D %b %Y') AS 'maturity_date', '{i['invoice_number']}' as `invoice_number`, '{i['bank_name']}' as `bank_name`,
							CURDATE() as 'start_date', DATE_ADD(CURDATE(), INTERVAL 2 MONTH) as 'end_date', tc.name_for_print from `tabUser Permission` tup 
							left join tabUser tu on tu.name = tup.`user`
							left join tabCompany tc on tc.name = tup.for_value
							where for_value = '{i['company_name']}'
							and tu.custom_user_type = "CUT-0001"
							group by tup.`user`,tu.custom_user_type
						"""
						# print(user_query)
						user_res = session.execute(user_query).fetchall()
						recipient_list += user_res
					bulk_insert_string = ''
					if recipient_list:
						for recp in recipient_list:
							user_data_dict = {   
								"user_email": recp['user'],
								"maturity_date": recp['maturity_date'],
								"full_name": recp['full_name'],
								"company": recp['name_for_print'],
								"bank_name": recp['bank_name'],
								"payer_code": recp['payer_code'], 
								"invoice_number": recp['invoice_number'], 
							} #### also content_modification_value
							user_data_string = json.dumps(user_data_dict)
							updated_notic_subject = notice_subject.replace("{{maturity_date}}",recp['maturity_date']).replace("{{company}}",recp['name_for_print'])
							bulk_insert_string += f"""(uuid(), now(), now(), '{owner}', '{owner}', '{notice_name}', '{recp['level']}', '{recp['user']}', '{updated_notic_subject}', '{recp['level_value']}', 0, NULL, '{noti_type}', '{recp['start_date']}', '{recp['end_date']}', '{user_data_string}'),"""
							del_user_list.append(recp['user'])
							email_recp_list.append(user_data_dict)
						bulk_insert_string = bulk_insert_string[:-1]
					# print(bulk_insert_string)
					del_user_tuple = "(" + ", ".join(['"' + item + '"' for item in del_user_list]) + ")"
					old_noti_del_query = f""" delete from `tabUser Notifications` where user_email in {del_user_tuple} and creation <= DATE_ADD(creation, INTERVAL 2 MONTH) and notification_type = "Auto" and notification_id = '{Constants.Maturity_Report_Notification_Master.value}'"""
					session.execute(old_noti_del_query)
					session.commit()
					bulk_insert_query = f"""
						INSERT INTO `tabUser Notifications`
						(name, creation, modified, modified_by, owner, notification_id,  parenttype, user_email, notification_description, level_value, has_read, read_on, notification_type, start_date, end_date, content_modification_value)
						VALUES {bulk_insert_string}
					"""
					# print(bulk_insert_query)
					session.execute(bulk_insert_query)
					session.commit()
				# print(email_recp_list)
				response = 'Success'
			else:
				response = 'Failed'
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in maturity_report_notification_processor:', str(Ex))
			response = 'Failed'
		print("maturity_report_notification_processor success")
		return response
	

	@classmethod 
	async def send_discount_scheme_email(self,**noti_data):
		try:
			print(noti_data)
			session = SessionMDB()
			print("processing")
			statusCode = 500
			att_status = 'Failed'
			response = ''
			comments = ''
			level_res = []
			notification_id = noti_data.get('notification_id','')
			notification_description = noti_data.get('notification_description','')
			noti_content = noti_data.get('noti_content','')
			user_name = noti_data.get('user_name','')
			email_content = noti_data.get('email_content','')
			email_content = json.loads(email_content)
			level_query = f""" select parenttype,level_value  from `tabUser Notifications` tun where user_email = '{user_name}' and notification_id = '{notification_id}' and notification_description = '{notification_description}' order by tun.modified desc limit 1"""
			level_res = session.execute(level_query).fetchall()
			filepath = convert_html_to_pdf_weasyprint(notification_description, noti_content,email_content)
			generate_and_send_discount_scheme_email(session,user_name,email_content,filepath,level_res)
			att_status = 'Success'
			statusCode = 200
			if os.path.isfile(filepath):
				os.remove(filepath)   
			message = f"Successfully updated Customer Alerts."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in reset_tax_dec_response:', str(Ex))
			message = str(Ex)
			att_status = 'Failed'
			statusCode = 500
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response
		
	@classmethod
	async def process_bank_credit_Standard_Chartered(self, **file_data):
		try:
			print("processing process_bank_credit_Standard_Chartered")
			statusCode = 500
			session = SessionMDB()
			att_status = 'Failed'
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			att_status = process_bank_credit_files_sc(session,file_data)
			if att_status == 'Success':
				bank_limit_holiday_updator(session)
				# await self.credit_notification_processor()
				statusCode = 200
				message = "Successfully updated SC credit data."
			else:
				statusCode = 500
				att_status = 'Failed'
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_bank_credit_Standard_Chartered:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response
	

	@classmethod 
	async def process_bank_discount_Standard_Chartered(self,**file_data):
		try:
			session = SessionMDB()
			print("processing process_bank_discount_Standard_Chartered")
			statusCode = 500
			att_status = 'Failed'
			response = ''
			comments = ''
			filename = file_data.get('file_name','')
			bank_name = file_data.get('bank_name','')
			att_status = process_maturity_report_bank_data(session,file_data,bank_name)
			if att_status == 'Success':
				await self.maturity_report_notification_processor(bank_name)
				statusCode = 200
				print(f"Successfully updated Maturity Report Data for {bank_name} bank.")
				statusCode = 200
				message = f"Successfully updated payment entries and maturity report"
			else:
				att_status = 'Failed'
				statusCode = 500
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_bank_discount_Axis:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response

	@classmethod
	async def process_bank_credit_Common(self, **file_data):
		try:
			print("processing process_bank_credit_common")
			statusCode = 500
			session = SessionMDB()
			att_status = 'Failed'
			response = ''
			comments = ''
			bank_name = file_data.get('bank_name','')
			data = file_data.get('data','')
			country = file_data.get('country','')
			username = file_data.get('user_name','')
			filename = file_data.get('file_name','')
			att_status = process_bank_credit_files_Common(session,data,bank_name,country,username)
			if att_status == 'Success':
				bank_limit_holiday_updator(session)
				# await self.credit_notification_processor()
				statusCode = 200
				message = "Successfully updated common credit data."
			else:
				statusCode = 500
				att_status = 'Failed'
				message = "Failed, please check the uploaded file."
		except Exception as e:
			Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
			print('Error in process_bank_credit_common:', str(Ex))
			att_status = 'Failed'
			statusCode = 500
			comments = str(Ex)
			message = "Failed, please check the uploaded file."
		filepath = file_data.get('file_path','')
		exec_str = file_status_updator(filename,filepath,att_status,comments)
		session.execute(exec_str)
		response = {
			'statusCode' : statusCode,
			'message' : message,
			'att_status' : att_status
		}
		session.commit()
		print(att_status)
		return response	