from os import environ

from databases import Database

from .. import dbs

DB_ADDRESS = environ.get("DB_ADDRESS", "sqlite:///./test.db")

dbs.create_db(DB_ADDRESS)
database = Database(DB_ADDRESS)
