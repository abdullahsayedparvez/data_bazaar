import os
from typing import NamedTuple

from dotenv import load_dotenv

load_dotenv()


class Config(NamedTuple):
    HOST_SQL: str
    PORT_SQL: str
    USERNAME_SQL: str
    PASSWORD_SQL: str
    DATABASE_SQL: str
    DATA_FOLDER : str

CONFIG = Config(
    HOST_SQL=os.getenv("HOST_SQL"),
    PORT_SQL=os.getenv("PORT_SQL"),
    USERNAME_SQL=os.getenv("USERNAME_SQL"),
    PASSWORD_SQL=os.getenv("PASSWORD_SQL"),
    DATABASE_SQL=os.getenv("DATABASE_SQL"),
    DATA_FOLDER = os.getenv("DATA_FOLDER")
)
print(CONFIG.HOST_SQL)