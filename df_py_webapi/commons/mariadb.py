from os import environ
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config.config import config

dbPath = config['DB']['MARIA_DB']
engine = create_engine(dbPath,pool_pre_ping=True)

SessionMDB = sessionmaker(bind=engine)

BaseMDB = declarative_base()
