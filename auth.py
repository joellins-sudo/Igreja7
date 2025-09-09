
from passlib.context import CryptContext
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
def hash_password(p:str)->str: return pwd_context.hash(p)
def verify_password(p:str,h:str)->bool: return pwd_context.verify(p,h)
