import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv
import os

load_dotenv()
KEY_PATH = os.getenv("SECRETS_PATH")

class FakeStr(str):
    def __str__(self):
        return "****"
    def __repr__(self):
        return "****"
    
def load_key():
    with open(KEY_PATH, "rb") as f:
        return f.read()
    
def decrypt_password(encrypted_password):
    f = Fernet(load_key())
    return FakeStr(f.decrypt(encrypted_password).decode())