import typing as ty
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.exc import IntegrityError
from models import Base

class Connection:
    def __init__(self, uri: str, table: DeclarativeBase):
        try:
            self.engine = create_engine(uri)
            self.table = table
            self.Session = sessionmaker(bind=self.engine)
            Base.metadata.create_all(self.engine)

            print(f"Connection Established to {uri}")

        except:
            raise ConnectionError("Invalid URI String")
    
    def insert_value(self, values: ty.Dict[str,ty.Any]) -> None:
            session = self.Session()
            print("inserting values... ")
            try:
                session.execute(
                        insert(self.table),
                        values
                )
                session.commit()
            except IntegrityError:
                    print(f"Entry {values} Already added")
                    session.rollback()
    
    def entry_exists(self, filename: str) -> bool:
        session = self.Session()
        exists = session.query(self.table).filter_by(filename=filename).first()

        return True if exists else False 