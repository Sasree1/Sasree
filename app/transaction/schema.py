from pydantic import BaseModel, EmailStr
from datetime import datetime
from typing import Optional


class UserQuery(BaseModel):
    query: str
    
    class Config:
        from_attributes = True

