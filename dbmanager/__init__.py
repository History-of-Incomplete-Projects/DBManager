from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.attributes import InstrumentedAttribute

from sqlalchemy import Column  # noqa
from sqlalchemy import ForeignKey  # noqa
from sqlalchemy import ForeignKeyConstraint  # noqa
from sqlalchemy import UniqueConstraint  # noqa
from sqlalchemy import func  # noqa
from sqlalchemy import ARRAY  # noqa
from sqlalchemy import BIGINT  # noqa
from sqlalchemy import BigInteger  # noqa
from sqlalchemy import BINARY  # noqa
from sqlalchemy import BLOB  # noqa
from sqlalchemy import BOOLEAN  # noqa
from sqlalchemy import Boolean  # noqa
from sqlalchemy import CHAR  # noqa
from sqlalchemy import CLOB  # noqa
from sqlalchemy import DATE  # noqa
from sqlalchemy import Date  # noqa
from sqlalchemy import DATETIME  # noqa
from sqlalchemy import DateTime  # noqa
from sqlalchemy import DECIMAL  # noqa
from sqlalchemy import Enum  # noqa
from sqlalchemy import FLOAT  # noqa
from sqlalchemy import Float  # noqa
from sqlalchemy import INT  # noqa
from sqlalchemy import INTEGER  # noqa
from sqlalchemy import Integer  # noqa
from sqlalchemy import Interval  # noqa
from sqlalchemy import JSON  # noqa
from sqlalchemy import LargeBinary  # noqa
from sqlalchemy import NCHAR  # noqa
from sqlalchemy import NUMERIC  # noqa
from sqlalchemy import Numeric  # noqa
from sqlalchemy import NVARCHAR  # noqa
from sqlalchemy import PickleType  # noqa
from sqlalchemy import REAL  # noqa
from sqlalchemy import SMALLINT  # noqa
from sqlalchemy import SmallInteger  # noqa
from sqlalchemy import String  # noqa
from sqlalchemy import TEXT  # noqa
from sqlalchemy import Text  # noqa
from sqlalchemy import TIME  # noqa
from sqlalchemy import Time  # noqa
from sqlalchemy import TIMESTAMP  # noqa
from sqlalchemy import TypeDecorator  # noqa
from sqlalchemy import Unicode  # noqa
from sqlalchemy import UnicodeText  # noqa
from sqlalchemy import VARBINARY  # noqa
from sqlalchemy import VARCHAR  # noqa


class DBManager:
    """[summary]
    """
    def __init__(self, path_to_db: str) -> None:
        """[summary]

        Args:
            path_to_db (str): [description]
        """
        self.engine = create_engine(path_to_db)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.Base = declarative_base()

    def build_all_tables(self) -> None:
        """[summary]
        """
        self.Base.metadata.create_all(self.engine)

    def get_or_create(self, model: type, **kwargs):
        """[summary]

        Args:
            model (type): [description]

        Returns:
            [type]: [description]
        """
        instance = self.get_any(model, **kwargs)
        if instance:
            return instance
        else:
            self.create_object(model, **kwargs)

    def create_object(self, model: type, **kwargs):
        """[summary]

        Args:
            model (type): [description]

        Returns:
            [type]: [description]
        """
        params = {
            k: v for k, v in kwargs.items()
            if not isinstance(v, ClauseElement)
        }
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
        """[summary]

        Args:
            model (type): [description]

        Returns:
            [type]: [description]
        """
        return self.session.query(model).filter_by(**kwargs).one_or_none()

    def get_all(self, model: type, **kwargs) -> list:
        """[summary]

        Args:
            model (type): [description]

        Returns:
            list: [description]
        """
        return self.session.query(model).filter_by(**kwargs).all()

    def exists(self, model: type, **kwargs) -> bool:
        """[summary]

        Args:
            model (type): [description]

        Returns:
            bool: [description]
        """
        if self.get_any(model, **kwargs):
            return True
        return False

    def get_max(self, model: type, column: InstrumentedAttribute, **kwargs):
        """[summary]

        Args:
            model (type): [description]
            column (InstrumentedAttribute): [description]

        Returns:
            [type]: [description]
        """
        subquery = self.session.query(func.max(column)).filter_by(**kwargs)
        query = self.session.query(func.max(column)).filter_by(
            getattr(model, column) == subquery, **kwargs)
        return query.first()

    def update(self, model: type, filters={}, updates={}) -> None:
        """[summary]

        Args:
            model (type): [description]
            filters (dict, optional): [description]. Defaults to {}.
            updates (dict, optional): [description]. Defaults to {}.
        """
        self.session.query(model).filter_by(**filters).update(**updates)
        self.commit()

    def update_row(self, row: object, **kwargs) -> None:
        """[summary]

        Args:
            row (object): [description]
        """
        for key in kwargs:
            setattr(row, key, kwargs[key])
        self.commit()

    def clear(self, model: type) -> int:
        """[summary]

        Args:
            model (type): [description]

        Returns:
            int: [description]
        """
        r = self.session.query(model).delete()
        self.commit()
        return r

    def get_first(self, model: type, **kwargs):
        """[summary]

        Args:
            model (type): [description]

        Returns:
            [type]: [description]
        """
        return self.session.query(model).filter_by(**kwargs).first()

    def commit(self):
        """[summary]
        """
        self.session.commit()
