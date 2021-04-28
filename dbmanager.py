from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.attributes import InstrumentedAttribute

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy import ARRAY
from sqlalchemy import BIGINT
from sqlalchemy import BigInteger
from sqlalchemy import BINARY
from sqlalchemy import BLOB
from sqlalchemy import BOOLEAN
from sqlalchemy import Boolean
from sqlalchemy import CHAR
from sqlalchemy import CLOB
from sqlalchemy import DATE
from sqlalchemy import Date
from sqlalchemy import DATETIME
from sqlalchemy import DateTime
from sqlalchemy import DECIMAL
from sqlalchemy import Enum
from sqlalchemy import FLOAT
from sqlalchemy import Float
from sqlalchemy import INT
from sqlalchemy import INTEGER
from sqlalchemy import Integer
from sqlalchemy import Interval
from sqlalchemy import JSON
from sqlalchemy import LargeBinary
from sqlalchemy import NCHAR
from sqlalchemy import NUMERIC
from sqlalchemy import Numeric
from sqlalchemy import NVARCHAR
from sqlalchemy import PickleType
from sqlalchemy import REAL
from sqlalchemy import SMALLINT
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import TEXT
from sqlalchemy import Text
from sqlalchemy import TIME
from sqlalchemy import Time
from sqlalchemy import TIMESTAMP
from sqlalchemy import TypeDecorator
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy import VARBINARY
from sqlalchemy import VARCHAR


class DBManager:
    def __init__(self) -> None:
        self.engine = create_engine(
            'sqlite:///game_play.db',
            connect_args={"check_same_thread": False}
        )
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def build_all_tables(self) -> None:
        self.BASE.metadata.create_all(self.engine)

    def get_or_create(self, model: type, **kwargs):
        instance = self.get_any(model, **kwargs)
        if instance:
            return instance
        else:
            self.create_object(model, **kwargs)

    def create_object(self, model: type, **kwargs):
        params = {k: v for k, v in kwargs.items() if not isinstance(v, ClauseElement)}
        instance = model(**params)
        try:
            self.session.add(instance)
            self.commit()
        except Exception:
            self.session.rollback()
            instance = self.session.query(model).filter_by(**kwargs).one()
            return instance, False
        else:
            return instance, True

    def get_any(self, model: type, **kwargs):
        return self.session.query(model).filter_by(**kwargs).one_or_none()

    def get_all(self, model: type, **kwargs) -> list:
        return self.session.query(model).filter_by(**kwargs).all()

    def exists(self, model: type, **kwargs) -> bool:
        if self.get_any(model, **kwargs):
            return True
        return False

    def get_max(self, model: type, column: InstrumentedAttribute, **kwargs):
        subquery = self.session.query(func.max(column)).filter_by(**kwargs)
        query = self.session.query(func.max(column)).filter_by(**kwargs, getattr(model, column) == subquery)
        return query.first()

    def update(self, model: type, filters={}, updates={}) -> None:
        objs = self.session.query(model).filter_by(**filters).update(**updates)
        self.commit()

    def update_row(self, row: object, **kwargs) -> None:
        for key in kwargs:
            setattr(row, key, kwargs[key])
        self.commit()

    def clear(self, model: type) -> int:
        r = self.session.query(model).delete()
        self.commit()
        return r

    def get_first(self, model: type, **kwargs): 
        return self.session.query(model).filter_by(**kwargs).first()

    def commit(self):
        self.session.commit()


Base = declarative_base()