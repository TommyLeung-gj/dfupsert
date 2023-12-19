from sqlalchemy import *
from sqlalchemy.orm import DeclarativeBase
from pandas import DataFrame
from dfupsert import upsert


class AbstractTable(DeclarativeBase):
    __abstract__ = True


# create a table
class TestTable(AbstractTable):
    __tablename__ = "test_table"
    __table_args__ = (
        UniqueConstraint("name"),
        {
            "extend_existing": True,
            "schema": "test"
        }
    )

    name = Column(String(255), primary_key=True)
    pwd = Column(String(255))
    signals = Column(String(100))


# make a dataframe
data = {
    "name": ["aaba", "c", "e"],
    "pwd": ["131", "3", "4"],
    "signals": ["a", 'v1', "1154"],
    "additional": ["a", 'v1', "11234"]
}
df = DataFrame(data)

# create a engine
engine = create_engine('mysql+pymysql://root:Mysql_1331@192.168.131.3:3306/test', echo=True)

# dfupsert
with engine.connect() as conn:
    upsert(
        df=df,
        con=conn,
        table=TestTable,
        chunksize=2000
    )
    conn.commit()
