import base64
import csv
from email import encoders, message
from email.mime.base import MIMEBase
import imaplib
import json
import shutil
import pysftp
import pytz
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from httpx import get
from urllib.parse import urljoin
from datetime import datetime, timedelta
from six import iteritems, text_type
from msal import ClientApplication
import pyzipper
import boto3
from botocore.exceptions import NoCredentialsError
import stat
from dfenum import Country, TimeZone


def getTabSeriesNumber(option, Session):
	nameIncrementSPData = 0
	try:
		query = "select current FROM tabSeries WHERE NAME = '" + option + "'"
		seriesNameSP8 = Session.execute(query)
		nameIncrementSPData = ''
		for data in seriesNameSP8:
			seriesNameSP = data[0]
			nameIncrementSPData = seriesNameSP
	except Exception as e:
		print('Error in getTabSeriesNumber method:', str(e))
	return nameIncrementSPData

def updateTabSeries(option, Session, nameIncrementSP):
	try:
		if nameIncrementSP != 0:
			query = f"UPDATE tabSeries SET CURRENT = '{nameIncrementSP}' WHERE NAME = '{option}'"
			Session.execute(query)
	except Exception as e:
		print('Error updateTabSeries method:', str(e))


def getdfconfigdetails():
	configdata = ''
	try:
		templates_dir = os.path.dirname(__file__)
		file_path = os.path.join(templates_dir, "dfconfig.cfg")
		configdata = ''
		with open(file_path) as file_handle:
			configdata = file_handle.read()
		if configdata == '':
			print('Invalid config data')
		else:
			configdata = json.loads(str(configdata))
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in getdfconfigdetails method: ', Except)
	return configdata

def imap_connector(sessions, jsondata):
	try:
		username = ""
		password = ""
		emailsmtp = ""
		emailserverdetails = []
		sessions.begin()
		emailserverdetails = getemaildetails(sessions, jsondata)
		if len(emailserverdetails) == 0:
			print("Invalid Email Credentials")
			return "Invalid Email Credentials"
		elif len(emailserverdetails) > 0:
			for item in emailserverdetails:
				username = item["emailid"]
				emailsmtp = item["smtpserver"]
				password = item["password"]
		imap = imaplib.IMAP4_SSL(emailsmtp)
		result = imap.login(username, password)
		if result[0] == 'OK':
			return imap
		else:
			return result
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in imap_connector method: ', Except)

def getbasepathdetails():
	configdata = ''
	try:
		templates_dir = os.path.dirname(__file__)
		file_path = os.path.join(templates_dir, "dfconfig.cfg")
		configdata = ''
		with open(file_path) as file_handle:
			configdata = file_handle.read()
		if configdata == '':
			print('Invalid config data')
		else:
			configdata = json.loads(str(configdata))
			basePath = configdata['basePath']
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in getbasepathdetails method: ', Except)
	return basePath


def get_session(session, jsondata):
	try:
		if not jsondata or jsondata == '':
			jsondata = getdfconfigdetails()
		dbPath = jsondata['dburl']
		if dbPath.strip(' ') == '':
			print("Not a valid connection string")
		if not session:
			engine = create_engine(dbPath)
			SessionMDB = sessionmaker(bind=engine)
			session = SessionMDB()
		return session
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in getSession method: ', Except)


def getemaildetails(session, jsondata):
	basePath = getbasepathdetails()
	sessions = get_session(session, jsondata)
	emailserverdetails = []
	try:
		query = f"""SELECT field, IFNULL(value,'') AS value FROM tabSingles WHERE doctype ='Digital Finance Settings' AND field IN ('di_smtp_email_address','di_smtp_port','di_smtp_server')"""
		result = sessions.execute(query)
		res_dict = {}
		for field, value in result:
			res_dict[field] = value
		emailid = res_dict['di_smtp_email_address']
		smtpserver = res_dict['di_smtp_server']
		smtpport = res_dict['di_smtp_port']

		url = urljoin(basePath,f"""/api/method/frappe.api.get_email_account_password?doctype=Digital Finance Settings&name=Digital Finance Settings&fieldname=di_smtp_email_password""")
		res = get(url)
		if res.status_code == 200:
			mydict = json.loads(res.text)
			for i in mydict:
				password = mydict[i]

		if emailid.replace(' ', '') != '' and password.replace(' ', '') != '' and smtpserver.replace(' ', '') != '' and smtpport.replace(' ', '') != '':
			_list = {
				"emailid": emailid,
				"password": password,
				"smtpserver": smtpserver,
				"smtpport": smtpport
			}
			emailserverdetails.append(_list)
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in common->getemaildetailsForDiscounts method: ', Ex)
	finally:
		sessions.close()
		return emailserverdetails


def sendEmail(subject,msg,to,filename,filepath, session,jsondata):
	emailserverdetails = getemaildetails(session, jsondata)
	if len(emailserverdetails) == 0:
		print("Invalid Email Credentials")
		return "Invalid Email Credentials"
	elif len(emailserverdetails) > 0:
		for item in emailserverdetails:
			username = item["emailid"]
			emailsmtp = item["smtpserver"]
			password = item["password"]

	# Mail template
	message = MIMEMultipart("alternative")
	message["Subject"] = subject
	message["From"] = username
	message["To"] = ", ".join(to)
	email_text = msg
	msg_part = MIMEText(email_text, "plain")
	message.attach(msg_part)
	attachment = open(filepath,"rb")
	p = MIMEBase('application', 'octet-stream')
	p.set_payload((attachment).read())
	encoders.encode_base64(p)
	p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
	message.attach(p)
	port = 587
	
	try:
		smtp_server = smtplib.SMTP(emailsmtp, port)
		smtp_server.connect(emailsmtp,port)
		smtp_server.ehlo()
		smtp_server.starttls()
		smtp_server.ehlo()
		smtp_server.login(username, password)
		smtp_server.sendmail(username, to, message.as_string())
		smtp_server.close()
		print("Email sent successfully!")
	except Exception as ex:
		print("An exception has occured in sendEmail: ", ex)

def sendHtmlEmail(subject,msg,to,cc, session,jsondata):
	emailserverdetails = getemaildetails(session, jsondata)
	if len(emailserverdetails) == 0:
		print("Invalid Email Credentials")
		return "Invalid Email Credentials"
	elif len(emailserverdetails) > 0:
		for item in emailserverdetails:
			username = item["emailid"]
			emailsmtp = item["smtpserver"]
			password = item["password"]
	# Mail template
	message = MIMEMultipart("alternative")
	message["Subject"] = subject
	message["From"] = username
	message["To"] = to
	message["Cc"] = cc
	email_text = msg
	msg_part = MIMEText(email_text, "html")
	message.attach(msg_part)
	port = 587
	try:
		if cc != '':
			full_cc = cc.split(',')
			toaddrs = [to]
			for fcc in full_cc:
				toaddrs += [fcc]
		else:
			toaddrs = to.split(',')
		smtp_server = smtplib.SMTP(emailsmtp, port)
		smtp_server.connect(emailsmtp,port)
		smtp_server.ehlo()
		smtp_server.starttls()
		smtp_server.ehlo()
		smtp_server.login(username, password)
		smtp_server.sendmail(username, toaddrs, message.as_string())
		smtp_server.close()
		print("Email sent successfully!")
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print("Error in sendHtmlEmail method: ", Ex)

def sendEmail_with_attachment(session,jsondata,subject,msg,to,fname=None, fcontent=None, content_type=None,inline=None,content_id=None):
	emailserverdetails = getemaildetails(session, jsondata)
	if len(emailserverdetails) == 0:
		return "Invalid Email Credentials"
	elif len(emailserverdetails) > 0:
		for item in emailserverdetails:
			username = item["emailid"]
			emailsmtp = item["smtpserver"]
			password = item["password"]

	"""Add attachment to parent which must an email object"""
	from email.mime.audio import MIMEAudio
	from email.mime.base import MIMEBase
	from email.mime.image import MIMEImage
	from email.mime.text import MIMEText

	import mimetypes
	if fname:
		if not content_type:
			content_type, encoding = mimetypes.guess_type(fname)

		if content_type is None:
			# No guess could be made, or the file is encoded (compressed), so
			# use a generic bag-of-bits type.
			content_type = 'application/octet-stream'

		maintype, subtype = content_type.split('/', 1)
		if maintype == 'text':
			# Note: we should handle calculating the charset
			if isinstance(fcontent, text_type):
				fcontent = fcontent.encode("utf-8")
			part = MIMEText(fcontent, _subtype=subtype, _charset="utf-8")
		elif maintype == 'image':
			part = MIMEImage(fcontent, _subtype=subtype)
		elif maintype == 'audio':
			part = MIMEAudio(fcontent, _subtype=subtype)
		else:
			part = MIMEBase(maintype, subtype)
			part.set_payload(fcontent)
			# Encode the payload using Base64
			from email import encoders
			encoders.encode_base64(part)		
		# Set the filename parameter
		if fname:
			attachment_type = 'inline' if inline else 'attachment'
			part.add_header('Content-Disposition', attachment_type, filename=text_type(fname))
		if content_id:
			part.add_header('Content-ID', '<{0}>'.format(content_id))

	message = MIMEMultipart("alternative")
	message["Subject"] = subject
	message["From"] = username
	message["To"] = ", ".join(to)
	email_text=msg
	msg_part = MIMEText(email_text, "html")
	message.attach(msg_part)
	if fname:
		message.attach(part)

	port = 587

	
	try:
		toaddrs = to
		smtp_server = smtplib.SMTP(emailsmtp, port)
		smtp_server.connect(emailsmtp,port)
		smtp_server.ehlo()
		smtp_server.starttls()
		smtp_server.ehlo()
		smtp_server.login(username, password)
		smtp_server.sendmail(username, toaddrs, message.as_string())
		smtp_server.close()
		print("Email sent successfully!")
	except Exception as ex:
		print("An exception has occured in sendEmail_with_attachment: ", ex)


def getfilepathdetails(session, jsondata, country_name=None):
	filepathdetails = []
	try:
		default_country_folder = Country.DefaultCountryFolder.value
		sessions = get_session(session, jsondata)
		query = """SELECT field, IFNULL(value,'') AS value FROM tabSingles WHERE doctype ='Digital Finance Settings' AND field IN ('file_server_host','file_download_path','processed_file_path','error_file_path','working_of_discount_email_subject')"""
		dfSettings = sessions.execute(query)
		dict = {}
		if country_name is None:
			country_name = default_country_folder   #### default str which will be replaced in code
		for field, value in dfSettings:
			dict[field] = value
		_list = {
			"file_download_path": dict['file_download_path'],
			"processed_files_path": dict['processed_file_path'].replace(default_country_folder, country_name),
			"error_files_path": dict['error_file_path'].replace(default_country_folder, country_name),
			"EmailSubject": dict['working_of_discount_email_subject']
		}
		filepathdetails.append(_list)
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in getfilepathdetailsForDiscounts method: ', Ex)
	finally:
		sessions.close()
		return filepathdetails

def get_downloads_path_details(session):
	try:
		query = """SELECT field, IFNULL(value,'') AS value 
					FROM tabSingles 
					WHERE doctype ='Digital Finance Settings' 
					AND field = 'file_download_path'"""
		res = session.execute(query)
		for field, value in res:
			if value:
				return value 
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in get_downloads_path_details method:', Ex)
		# You might want to log the exception instead of printing
	return ""

def move_email_to_folder(smtp,imap,mail_folder,msg_id):
	try:
		if(smtp == 'smtp.gmail.com'):
			# tags the mail with the specified table
			result = imap.store(msg_id, '+X-GM-LABELS', mail_folder)
			if result and result[0] == 'OK':
				# flag the mail for deletion if the copy was successful
				imap.store(msg_id, '+FLAGS', '(\Deleted)')
		else:
			# copy the mail into the "mail_folder"
			result = imap.copy(msg_id, mail_folder)
			if result and result[0] == 'OK':
				# flag the mail for deletion if the copy was successful
				imap.store(msg_id, '+FLAGS', '(\Deleted)')

	except Exception as e:
		err = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in move_email_to_folder method :',err)


def add_DigitalFinanceLog(session, log_name, creation, name_series, message, method_name, log_comment):
	try:
		log_query = f"""insert into `tabDigital Finance Log` (name,creation, modified, modified_by, owner, naming_series, message, program_name, comment) VALUES ('{log_name}','{creation}','{creation}', 'Administrator', 'Administrator', '{name_series}', '{message}', '{method_name}', '{log_comment}' )"""
		session.execute(log_query)
		print("Successfully inserted into logs")
	except Exception as e:
		err = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in addDigitalFinanceLog method :',err)


def is_same_financial_year(existing_date, adding_date):
	try:
		date1_fisc_start_date_str = f"01-04-{existing_date.year}"
		date1_fisc_end_date_str = f"31-03-{existing_date.year+1}"
		
		if existing_date.month < 4:
			date1_fisc_start_date_str = f"01-04-{existing_date.year-1}"
			date1_fisc_end_date_str = f"31-03-{existing_date.year}"

		date1_fisc_start_date = datetime.strptime(date1_fisc_start_date_str, "%d-%m-%Y")
		date1_fisc_end_date = datetime.strptime(date1_fisc_end_date_str, "%d-%m-%Y")

		if date1_fisc_start_date <= adding_date <= date1_fisc_end_date:
			return True
		else:
			return False
	except:
		pass
	return False
	
	
def get_password(doctype,setting_name,fieldname):
	password = ''
	try:
		jsondata = getdfconfigdetails()
		if jsondata == '':
			print('Invalid config data')
			return 'Invalid config data'
		basePath = jsondata['basePath']
		url=urljoin(basePath,"/api/method/frappe.api.get_email_account_password?doctype=" + doctype +"&name="+setting_name +"&fieldname=" + fieldname)
		res = requests.get(url)        
		if res.status_code == 200:
			mydict = json.loads(res.text)
			for i in mydict:
				password = mydict[i]        
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in get_password method:',str(Ex))
	return password

def getsftpdetails(session):
	sftp_connection = ''
	try:
		query = """SELECT field,value FROM `tabSingles` 
		WHERE doctype = 'digital finance settings' AND FIELD IN 
		('sftp_host', 'sftp_user_name','sftp_password','sftp_port','sftp_folder','sftp_processed_files_folder','sftp_error_files_folder')"""
		sftp_details = session.execute(query)
		sftp_dict = {} 
		for field,value in sftp_details: 
			sftp_dict[field] = value        
		my_host_name  = sftp_dict['sftp_host']
		print("SFTPHost: "+my_host_name)
		my_username = sftp_dict['sftp_user_name']
		sftp_port = sftp_dict['sftp_port']
		doctype= "Digital Finance Settings"
		setting_name= "Digital Finance Settings"
		fieldname= "sftp_password"
		my_password = get_password(doctype,setting_name,fieldname)        
		sftp_folder = sftp_dict['sftp_folder']
		cnopts = pysftp.CnOpts()
		cnopts.hostkeys = None 
		sftp_connection = pysftp.Connection(host=my_host_name,port= int(sftp_port),username=my_username, password=my_password,cnopts=cnopts)        
		sftp_connection.cwd(sftp_folder)       
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in getsftp_details method  :',str(Ex))
	return sftp_connection,sftp_dict

def process_and_move_files(file_path, file_name, sftp_connection, sftp_folder, sftp_error_path, sftp_processed_path , error_files_path, processed_files_path):
	if os.path.exists(file_path):

		# SFTP file movement
		if sftp_error_path:
			sftp_connection.put(file_path, f"{sftp_error_path}{file_name}")
		elif sftp_processed_path:
			sftp_connection.put(file_path, f"{sftp_processed_path}{file_name}")
		sftp_connection.remove(f"{sftp_folder}{file_name}")

		# Move the file to the appropriate folder
		if error_files_path:
			shutil.move(file_path, f"{error_files_path}{file_name}")
		elif processed_files_path:
			shutil.move(file_path, f"{processed_files_path}{file_name}")


def csvKeyValExtration(filename):
	try:
		infile = open(filename, "r")
		read = csv.reader(infile)
		headers = next(read)     
		returnList=[]
		for row in read:
			dd = dict(zip(headers, row))
			returnList.append(dd)
		return returnList
		
		# infile = open(filename, "r")
		# read = csv.DictReader(infile)
		# return read

	except Exception as e:
		err = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"       
		print('Error in excelExtraction method :',err)


############### GraphAPI Services ###################

def get_MS_GraphAPI_headers(client_id,tenent_id,username,password):  ### Returns the header with the Bearer token
	try:
		app = ClientApplication(client_id=f'{client_id}',authority=f'https://login.microsoftonline.com/{tenent_id}/')

		token = app.acquire_token_by_username_password(username=username, password=password,scopes=['.default'])

		return {"Authorization": f"Bearer {token.get('access_token')}"}
	except Exception as e:
		err = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"       
		print('Error in get_MS_GraphAPI_headers method :',err)

def get_MS_GraphAPI_headers_new(sessions,client_id,tenent_id,client_secret):  ### Returns the header with the Bearer token
	try:
		print("Generating new access token...")
		new_access_token = refresh_access_token(sessions,client_id,tenent_id,client_secret)
		return {"Authorization": f"Bearer {new_access_token}"}
	except Exception as e:
		err = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"       
		print('Error in get_MS_GraphAPI_headers_new method :',err)

def download_email_attachments_GraphAPI(GRAPH_API_ENDPOINT,parent_id,child_id,email_id, headers, file_name, save_folder=os.getcwd()):  ### downloads email attachments based on email folders
	try:
		file_names = []
		filename_extensions_to_ignore = (".png", ".jpg", ".pdf", ".jpeg")
		down_url = ''
		if child_id != '':
			down_url = f"""{GRAPH_API_ENDPOINT}{parent_id}/childFolders/{child_id}/messages/{email_id}/attachments/"""
		else:
			down_url = f"""{GRAPH_API_ENDPOINT}{parent_id}/messages/{email_id}/attachments/"""

		response = requests.get(down_url,headers=headers)

		attachment_items = response.json()['value']
		for attachment in attachment_items:
			if file_name != '' and file_name != 'WOD' and attachment['size'] > 1500000:   #### checks if an inv_pbc_p08 file is < 1.5mb, anything greater is faulty. 
				print(f"INV_PBC_P08 file {file_name} is greater than 1.5mb ({attachment['size']})")
			else:
				if file_name == '':
					if attachment['name'].lower().endswith(filename_extensions_to_ignore):
							continue
					file_name = attachment['name']
				elif file_name == 'WOD':
					file_name = attachment['lastModifiedDateTime'].replace(':','')+'_'+attachment['name']
				else:                                               ### for inv_pbc_p08
					file_name = file_name.split('-')
					e_string = file_name[1].split('_')
					m_sub = file_name[0]
					if len(e_string) == 4:
						file_name = m_sub + "-" + (attachment['name'].strip(' ').split('.'))[0] + "_" + e_string[3]+ '.pdf'
					else :
						file_name = m_sub + "-" + attachment['name'].strip(' ')
						if file_name.lower().endswith(filename_extensions_to_ignore):
								continue
				file_names.append(file_name)
				attachment_id = attachment['id']
				attachment_content = requests.get(
					f"""{down_url}{attachment_id}/$value""",
					headers=headers
				)
				print(f'Saving file {file_name}')
				with open(os.path.join(save_folder, file_name), 'wb') as _f:
					_f.write(attachment_content.content)
		return file_names
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in download_email_attachments:',str(Ex))

def capture_id(url, headers):   ### returms email folder's ID
	g_id = ''
	response = requests.get(url, headers=headers)
	if response.status_code == 200:
		json_data = response.json()
		g_id = json_data['value'][0]['id']
	return g_id

def move_processed_mails_GraphAPI(id_list,base_url,parent_id,child_id,sub_folder_id,headers): ### returms email folder's ID
	try:
		if child_id != '':
			for i in id_list['value']:
				unread_url = f"""{base_url}/{parent_id}/childFolders/{child_id}/messages/{i['id']}"""  
				requests.patch(unread_url, json = {'isRead': True}, headers = headers)                   ### marks the email as read
				move_url = f"""{unread_url}/move"""
				requests.post(move_url, json = {'destinationId': sub_folder_id}, headers = headers)       ### marks the email into the subfolder
		else:
			for i in id_list['value']:
				unread_url = f"""{base_url}/{parent_id}/messages/{i['id']}"""
				requests.patch(unread_url, json = {'isRead': True}, headers = headers)
				move_url = f"""{unread_url}/move"""
				requests.post(move_url, json = {'destinationId': sub_folder_id}, headers = headers)
		return 'Success'
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in move_processed_mails_GraphAPI:',str(Ex))
		return 'Failed'

def generate_email_body(toRecipients,ccRecipients,subject,contentType,content,importance,attachments):
	
	""" ###format of the body
	    request_body = {
	        'message': {
	            # recipient list
	            'toRecipients': [
	                {
	                    'emailAddress': {
	                        'address': '<recipient email address>'
	                    }
	                }
	            ],
	            # email subject
	            'subject': 'I am Spamming you',
	            'importance': 'normal',
	            'body': {
	                'contentType': 'HTML',
	                'content': '<b>Be Awesome, be spammed</b>'
	            },
	            # include attachments
	            'attachments': [
	                {
	                    '@odata.type': '#microsoft.graph.fileAttachment',
	                    'contentBytes': media_content.decode('utf-8'),
	                    'name': os.path.basename(file_path)
	                }
	            ]
	        }
	    }
	"""

	return {
				'message': {
					'toRecipients': toRecipients,
					"ccRecipients":ccRecipients,
					'subject': subject,
					'importance': importance,
					'body': {
						'contentType': contentType,
						'content': content
					},
					"attachments":attachments
				}
			}

def draft_attachment(file_path):
	if not os.path.exists(file_path):
		print('file is not found')
		return
	with open(file_path, 'rb') as upload:
		media_content = base64.b64encode(upload.read())
	data_body = {
		'@odata.type': '#microsoft.graph.fileAttachment',
		'contentBytes': media_content.decode('utf-8'),
		'name': os.path.basename(file_path)
	}
	return data_body


def refresh_access_token(sessions,client_id,tenant_id,client_secret):
	# Define the URL and parameters

	query="select value from tabSingles ts where doctype = 'azure_key' and field = 'refresh_token'"
	refresh_token = sessions.execute(query).fetchall()[0]['value']
	url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
	scope = "User.Read Mail.ReadWrite Mail.Send offline_access"

	# Create the data payload for the POST request
	data = {
		"client_id": client_id,
		"scope": scope,
		"refresh_token": refresh_token,
		"grant_type": "refresh_token",
		"client_secret": client_secret
	}

	# Make the POST request
	response = requests.post(url, data=data)

	# Check if the request was successful
	if response.status_code == 200:
		# print(response.json())
		# Parse the response and extract the new access token
		access_token = response.json().get("access_token")
		refresh_token = response.json().get("refresh_token")
		up_query = f"""
			update tabSingles set value = '{refresh_token}' where doctype = 'azure_key' and field = 'refresh_token'
		"""
		sessions.execute(up_query)
		sessions.commit()
		return access_token
	else:
		print("Error:", response.text)
		return None

def extractemail(session, filepathdetails,jsondata,client_id,tenent_id,email_folder,email_text,err_to_emails,GRAPH_API_ENDPOINT):
	try:
		emailserverdetails =[]
		username = ""
		password = "" 
		file_name = ''
		downloaded_files = []
		error_files_path = ""
		parent_id = ''
		child_id = ''
		file_download_path = ""
		emailserverdetails =getemaildetails(session, jsondata)
		if len(emailserverdetails) == 0:
			print("Invalid Email Credentials")
			return "Invalid Email Credentials"
		elif len(emailserverdetails) > 0:
			for item in emailserverdetails:
				username=item["emailid"]                    
				password = item["password"]
		
		if len(filepathdetails) == 0:
			print('Invalid filepathdetails')
			return
		for item in filepathdetails:
			file_download_path = item["file_download_path"]
			
		headers = get_MS_GraphAPI_headers(client_id,tenent_id,username,password)

		parentid_url = GRAPH_API_ENDPOINT+f"?$filter=displayName eq '{email_folder}'"
		parent_id = capture_id(parentid_url, headers)

		processedid_url = f"""{GRAPH_API_ENDPOINT}/{parent_id}/childFolders?$filter=displayName eq 'ProcessedMails'"""
		processed_id = capture_id(processedid_url, headers)

		url = f"""{GRAPH_API_ENDPOINT}{parent_id}/messages/?$filter=isRead+ne+true&top=1000""" 

		response = requests.get(url, headers=headers)
		if response.status_code != 200:
			raise Exception(response.json())

		response_json = response.json()
		response_json.keys()

		emails = response_json['value']
		for email in emails:
			if email['hasAttachments']:
				email_id = email['id']
				downloaded_files += download_email_attachments_GraphAPI(GRAPH_API_ENDPOINT,parent_id,child_id,email_id, headers,file_name, file_download_path)

		resp = move_processed_mails_GraphAPI(response_json,GRAPH_API_ENDPOINT,parent_id,child_id,processed_id,headers)
		if resp == 'False':
			print("Something went wrong with the file transfer operation")
			raise Exception(resp.json())     
									
		return downloaded_files
	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in extractemail method: ',Except)   
		html_c = f""" <html><p>{Except}</p></html> """
		sendHtmlEmail(email_text,html_c,err_to_emails,'', session,'')

############### GraphAPI Services END ###################


def get_fiscal_year(session,ext_sys,country):
	try:
		query = f""" select start_date,end_date  from `tabDF Fiscal Year` tdfy where external_system = '{ext_sys}' and country = '{country}' limit 1"""
		res = session.execute(query).fetchall()
		start_date = res[0]['start_date']
		end_date = res[0]['end_date'] 
		return start_date,end_date
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
		print('Error in get_fiscal_year:',str(Ex))
		

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
		hol_query = f""" select holiday_date from tabHoliday th where year(holiday_date)  = CAST(YEAR(CURRENT_DATE) AS CHAR(6)) order by holiday_date  """
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
	
def auto_notification_sender(webapi_base_path):
	try:   
		api_endpoint_url = f'{webapi_base_path}/dfwebapi/credit_notification_processor'
		return requests.post(api_endpoint_url)
	except Exception as e:
		Ex = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__} : {e}"
		print("Error in auto_notification_sender method: ", Ex)
		return str(e)

#endregion


#region extract_zip_with_password for bank credit limit SC
	
def extract_zip_with_password(zip_file_path, extract_to_path, password):
	try:
		extracted_files = []
		# Set permissions (e.g., 775 or 664)
		os.chmod(zip_file_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRGRP | stat.S_IROTH)
		with pyzipper.AESZipFile(zip_file_path, 'r', compression=pyzipper.ZIP_STORED, encryption=pyzipper.WZ_AES) as zf:
			zf.pwd = password.encode('utf-8')

			for file_info in zf.infolist():
				# Extract each file individually with the provided password
				zf.extract(file_info, path=extract_to_path)
				extracted_files.append(file_info.filename)

		print(f"Extraction successful. Files extracted to: {extract_to_path}")
		return True, extracted_files

	except Exception as e:
		Except = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}"
		print('Error in extract_zip_with_password method: ', Except)
		return False, []
	
#endregion
	

#region function to move files to s3 bucket
	
def upload_to_s3(local_folder, bucket_name, aws_access_key, aws_secret_key, region_name, file_type, doc_folder, country_name):
	"""
	Uploads all files from a local folder to an S3 bucket.
	Parameters:
	- local_folder: The local folder path containing the files to upload.
	- bucket_name: The name of your S3 bucket.
	- aws_access_key: Your AWS access key.
	- aws_secret_key: Your AWS secret key.
	- region_name: The AWS region where your S3 bucket is located.
	- file_type: Which core program is uing the file, ex. Airflow
	- doc_folder: S3 Sub Folder
	- country_name: self explanatory
	"""
	# Create an S3 client
	s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region_name)
	status = 'False'
	try:
		for root, dirs, files in os.walk(local_folder):
			for file in files:
				local_path = os.path.join(root, file)
				sub_folder = local_folder.split('/')[-2]
				s3_path = f"{doc_folder}/{country_name}/{file_type}/{sub_folder}/" + file
				s3.upload_file(local_path, bucket_name, s3_path)
				print(f'Successfully uploaded {local_path} to {bucket_name}/{s3_path}')
				status = 'Success'

	except FileNotFoundError:
		print(f'The file or directory {local_folder} does not exist.')
	except NoCredentialsError:
		print('Credentials not available or incorrect.')
	return status
	
#endregion


#region holiday checker
	
def holiday_checker(session, country_name, cur_date = None):
	### checks if the current day is a holiday (Duh...)
	holy_query = f"""
					SELECT * FROM `tabHoliday` th
					JOIN `tabHoliday List` thl 
					WHERE th.holiday_date = '{cur_date}' AND thl.`country` = '{country_name}'
				"""
	holy_res = session.execute(holy_query).fetchall()
	return holy_res
	
#endregion

def format_date(date_str):
	if date_str and date_str != '0' and date_str != '00000000':
		return f"'{datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')}'"
	else:
		return "NULL"
	
def get_timezone_from_country(country_name):
	try:
		country_tz = TimeZone.COUNTRY_TIMEZONES.value.get(country_name)	
	except Exception as e:
		print(f"Error in get_timezone_from_country, taking default timezone '{TimeZone.DefaultTZ.value}'")
		country_tz = TimeZone.DefaultTZ.value
	default_server_tz = pytz.timezone(TimeZone.DefaultServerTZ.value)
	ctz = pytz.timezone(country_tz)
	cur_datetime = (datetime.now().replace(tzinfo=default_server_tz).astimezone(ctz))
	return country_tz,cur_datetime

def get_country_from_code(session, c_code):
	country_name = ''
	country_query = f"select country from `tabExternal System Child` tesc where code = '{c_code}' limit 1"
	country_res = session.execute(country_query).fetchone()
	if country_res:
		country_name = country_res['country']
	return country_name