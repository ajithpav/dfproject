from enum import Enum
class BusinessArea(Enum):
     P08 = "ES-0001"
     PBC = "ES-0002"
     P4S = "ES-0003"
     ES0001 = "Debalb/RUP"
     ES0002 = "CP/Seeds/ES"
     ES0003 = "P4S"
     
class SupplierType(Enum):
     Bank = "SPT-0001"
     Sponsor = "SPT-0002"

class NamingSeries(Enum):
     TaxDeclarationName = 'TD-'
     TaxDeclarationSeries = 'TD-.#'
     UUID = 'UUID'

class UserAlertConstant(Enum):
     DFDateFormat = "%d-%m-%y"
     CurrentDateVariable = '&lt;current_date&gt;'
     PreviousDateVariable = '&lt;previous_date&gt;'
     TaxDeclarationLinkVariableHTML = '<tax_declaration_link>'
     TaxDeclarationLinkVariable = '&lt;tax_declaration_link&gt;'

class Constants(Enum):
     AdminUser = "Administrator"
     PaymentStatus_PIP = 'Payment in Progress'
     PaymentStatus_PPIP = 'Partial Payment in Progress'
     Paid_Text = 'Paid'
     Failed_Text = 'Failed'
     Approved_Text = 'Approved' 
     Tax_Declaration = 'AT-003'
     Credit_Expiry_Notification_Master = 'Master-NA-CreditLimit'
     Credit_Expiry_Email_Master = 'Bank Credit Limit Expiry Notification'
     Credit_Expired_Notification_Master = 'Master-NA-ExpiredCreditLimit'
     Credit_Expired_Email_Master = 'Bank Credit Limit Expired Notification'
     Discount_Scheme_Email_Template = "Discount Scheme Email Notification"
     Maturity_Report_Notification_Master = 'Master-NA-MarurityReport'
     Axis_bank_name = 'Axis'
     ICICI_bank_name = 'ICICI'
     SC_bank_name = 'Standard Chartered'
     Brac_bank_name = 'BRAC'
     City_bank_name = 'CITY'
     Supplier_Type = 'SPT-0001'
     TestPayerCodes = "('12345678','87654321')"