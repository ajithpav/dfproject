from encodings import utf_8
from tokenize import String
from Crypto.Cipher import AES

import base64, math, json
from config.config import config


class DFSecurity():
    
    def __init__(self):
        print("DFSecurity initialized")
    
    def get_common_cipher(self):
        return AES.new(config['SECURITY']['KEY'].encode("utf8"), AES.MODE_CBC, config['SECURITY']['IV'].encode("utf8"))
    
    
    def encrypt_with_common_cipher(self,cleartext):
        common_cipher = self.get_common_cipher()
        cleartext_length = len(cleartext)
        next_multiple_of_16 = 16 * math.ceil(cleartext_length/16)
        padded_cleartext = cleartext.rjust(next_multiple_of_16).encode("utf8")
        raw_ciphertext = common_cipher.encrypt(padded_cleartext)
        return base64.b64encode(raw_ciphertext).decode('utf8')
    
    def encryptData(self, content_to_encrypt):
        #check the type of the content
        if type(content_to_encrypt) is str :
            print("this is already in string format")
        elif type(content_to_encrypt) is dict:
            print("this is dictionary convert to string")
            content_to_encrypt = json.dumps(content_to_encrypt)
        else:
            print("not in string or dict, then converting to str")
            content_to_encrypt = str(content_to_encrypt)
        return self.encrypt_with_common_cipher(content_to_encrypt)
    
    def encryptDataWithResponse(self, status_code, message, content_to_encrypt):
        #check the type of the content
        if type(content_to_encrypt) is str :
            print("this is already in string format")
        elif type(content_to_encrypt) is dict:
            print("this is dictionary convert to string")
            content_to_encrypt = json.dumps(content_to_encrypt)
        else:
            print("not in string or dict, then converting to str")
            content_to_encrypt = str(content_to_encrypt)
        return  {
            'status' : status_code, 'message' : message,
            'content': self.encrypt_with_common_cipher(content_to_encrypt)}

    def decrypt_with_common_cipher(self, ciphertext):
        common_cipher = self.get_common_cipher()
        raw_ciphertext = base64.b64decode(ciphertext)
        decrypted_message_with_padding = common_cipher.decrypt(raw_ciphertext)
        return decrypted_message_with_padding.decode('utf8').strip()