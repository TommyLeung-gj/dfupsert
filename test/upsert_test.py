from sqlalchemy import *
from sqlalchemy.orm import DeclarativeBase
from pandas import DataFrame
from dfupsert import upsert


class AbstractTable(DeclarativeBase):
    __abstract__ = True


# create a table
class TestTable(AbstractTable):
    __tablename__ = "TestTable"
    __table_args__ = (
        UniqueConstraint("Name"),
        {
            "extend_existing": True,
            "schema": "dbo",
        }
    )

    name = Column("Name", String(255), primary_key=True)
    pwd = Column("Password", String(255))
    signals = Column("Signals", String(100))


# make a dataframe
data = {
    "Name": ["aaba", "c", "e"],
    "Password": ["131", "3", "3"],
    "Signals": ["abc", 'v1', "121154" * 2],
    "additional": ["a", 'v1', "1234"]
}
df = DataFrame(data)

# create a engine
engine = create_engine('mysql+pymysql://root:Mysql_1331@192.168.131.3:3306/test', echo=True)
engine = create_engine('postgresql+psycopg2://postgres:1331@192.168.131.3:5432/sync_with_alist', echo=True)
engine = create_engine("mssql+pymssql://sa:Mssql_1331@192.168.131.3:1433/easyform", echo=True)
# dfupsert
with engine.connect() as conn:
    upsert(
        df=df,
        con=conn,
        table=TestTable.__table__,
        chunksize=2000
    )
    conn.commit()
