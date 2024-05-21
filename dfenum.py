from enum import Enum
class DocumentType(Enum):
     RV = "TT-0001"
     VK = "TT-0002"
     DR = "TT-0003"
     DZ = "TT-0005"
     DA = "TT-0006"
     AB = "TT-0008"  
     DG = "TT-0011"
     RZ = "TT-0009"
     ZJ = "TT-0012"
     DP = "TT-0013"
     RW = "TT-0014" 
     DF = "TT-0015"
     KY = "TT-0016"
     DX = "TT-0017"
     CN = "Credit Notes"
     DN = "Debit Notes"
     INV = "Invoices"  
     TT0001 = "RV"
     TT0003 = "DR"
     TT0008 = "AB"
     
class NamingSeries(Enum):
     SOA = "SOA-"
     H2RD = "H2RD-"
     SP8D = "SP8D-"
     IDCPD = "IDCPD-"
     FPD = "FPD-"
     FE = "FE-"
     UNF = "UNF-"
     DFL = "DFL-"
     WOD = "WOD-"
     COB = "COB-"
     BCUD = "BCUD-"
     BDD = "BDD-" #Bank Discounting Dump
     CL = "CL-"
     SOA_NS = "SOA-.#####"
     SP8D_NS ="SP8D-.#####"
     IDCPD_NS = "IDCPD-.#"
     FE_NS = "FE-.#####"
     FPD_NS = "FPD-.#####"
     UNF_NS = "UNF-.#"
     DFL_NS = "DFL-.#"
     WOD_NS = "WOD-.#"
     COB_NS = "COB-.#"
     CL_NS = "CL-.#####"
     P = "P-"
     IP = "IP-"
     UUID = 'UUID'
     

class DFConstant(Enum):
     P08Name = 'ES-0001'
     PBCName = 'ES-0002'
     P4SName = 'ES-0003'
     P08 = "Dekalb/RUP"
     PBC = "CP/Seeds/ES"
     P4S = "P4S"
     TTOpeningBalanceName = 'TT-0010'
     AdminUser = 'Administrator'
     INV_CN_DN_EmailFolder = 'INV_CN_DN'
     CN_DN_WORKING_EmailFolder = 'CN_DN_WORKING'
     BANK_CREDIT_UTILISATION_EmailFolder = 'BANK_CREDIT_UTILISATION'
     ICICI_BANK_EmailFolder = 'DF_BANKREPORT_ICICI'
     SC_BANK_CREDIT_EmailFolder = 'DF_CREDITUTILIZATION_SC'
     AUTOMATE_MATURITY_REPORT_AXIS = 'DF_MATURITYREPORT_AXIS'
     SC_BANK_MATURE_EmailFolder = 'DF_MATURITYREPORT_SC'
     SC_BANK_MATURE_FILE_SHEET = 'Detailed'
     SC_BANK_DISCOUNT_EmailFolder = 'DF_DISCOUNTING_SC'
     BANK_DISCOUNTING_EmailFolder = "BANK_DISCOUNTING"
     City_BANK_Report_EmailFolder = "DF_BankReport_CITY" 
     Brac_BANK_Report_EmailFolder = "DF_BankReport_BRAC" 	 
     Brac_credit_sheet_name = 'Credit Utilization '
     Brac_maturity_sheet_name = 'Maturity ' 
     City_credit_sheet_name = 'Credit Utilization'
     City_maturity_sheet_name = 'Maturity'
     Bank_SupplierType_Name = "SPT-0001"
     Bank_SupplierType_Desc = "Bank"	  
     Bayer_SupplierType_Name = "SPT-0002"
     Bayer_SupplierType_Desc = "Bayer"
     Sponsor_SupplierType_Desc = "Sponsor"
     
     IP_Parent_Field = 'invoice_payment'
     IP_Parent_Type = 'Payment'
     Credit_note_PO8 = 'CN_P08'
     Credit_note_PBC = 'CN_PBC'
     Debit_Note_P08 = 'DN_P08'
     Debit_Note_PBC = 'DN_PBC'
     Invoice_P08 = 'INV_P08'
     Invoice_PBC = 'INV_PBC'
     PM_ChannelFinance_Name = 'PM-002'
     INV_CN_DN_Folder_List = ['INV_CN_DN', 'INV_CN_DN/CN_P08', 'INV_CN_DN/CN_PBC', 'INV_CN_DN/DN_P08', 'INV_CN_DN/DN_PBC', 'INV_CN_DN/INV_P08','INV_CN_DN/INV_PBC', 'INV_CN_DN/ProcessedMails', 'INV_CN_DN/ProcessedMails/PM_CN_P08','INV_CN_DN/ProcessedMails/PM_CN_PBC', 'INV_CN_DN/ProcessedMails/PM_DN_P08', 'INV_CN_DN/ProcessedMails/PM_DN_PBC', 'INV_CN_DN/ProcessedMails/PM_INV_P08', 'INV_CN_DN/ProcessedMails/PM_INV_PBC']
     WOD_Attachment_Type = 'WOD'
     PaymentStatus_PIP = 'Payment in Progress'
     PaymentStatus_PPIP = 'Partial Payment in Progress'
     PaymentStatus_Failed = 'Failed'
     H2R_SplitFolder = 'H2RSplitFiles/'
     SFTP_Daily_Files = ['FBL5N_PBC_OPENITEMS', 'H2R_Daily', 'FBL5N_P08_OPENITEMS', 'SOA_P08']
     Paid_Text = 'Paid'     
     Axis_bank_name = 'Axis'
     ICICI_bank_name = 'ICICI'
     SC_bank_name = 'Standard Chartered'
     Brac_bank_name = 'BRAC'
     City_bank_name = 'CITY'
     Supplier_Type = 'SPT-0001'
     SC_Credit_Extracted_Folder = 'SC_Credit'
     SC_Maturity_Extracted_Folder = 'SC_Maturity'
     TestPayerCodes = "('12345678','87654321')"
   

class AirFlowProgramName(Enum):
     SOA_P08 = "SOA_P08_Airflow"
     INV_PBC_P08 = 'Invoice_PBC_P08_Airflow'
     C_F_E_N = "Channel_Finance_Email_Notification"

class LogConstants(Enum):
     matched = 'Matched mails'
     unmatched = 'Unmatched mails'
     
     
class DBError(Enum):
     dublicate_entry = "Duplicate entry"

class ExternalSysLists(Enum):
     PBC_list = ['DBC', 'QBC', 'PBC']
     PO8_list = ['D08', 'Q08','P08']
     P4S_list = ['D4S', 'Q4N', 'P4S']
     SAP_SYSTEM_LIST = ['P08', 'Q08', 'PBC', 'QBC', 'P4S', 'Q4S', 'Q4N']

class BusinessArea(Enum):
     P08 = "ES-0001"
     PBC = "ES-0002"
     P4S = "ES-0003"
     ES0001 = "Dekalb/RUP"
     ES0002 = "CP/Seeds/ES"
     ES0003 = "P4S"

class CreditControl(Enum):
     CP = 'CCA-0001'
     SEEDS = 'CCA-0002'
     ES = 'CCA-0003'
     CCA0001 = 'CP'
     CCA0002 = 'Seeds'
     CCA0003 = 'ES'
     
class Country(Enum):
     India = "India"
     Bangladesh = "Bangladesh"
     DefaultCountryFolder = "_country_"

class TimeZone(Enum):
     DefaultServerTZ = "UTC"
     DefaultTZ = "Asia/Kolkata"
     COUNTRY_TIMEZONES = {
          'India': 'Asia/Kolkata',
          'Bangladesh': 'Asia/Dhaka',
          'Thailand': 'Asia/Bangkok'
     }  #### added here for now to maintain parity, can also be taken through packages.