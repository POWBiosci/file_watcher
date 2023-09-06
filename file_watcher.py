import os
import json
import logging
import typing as ty
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.exc import IntegrityError
from watchdog.events import LoggingEventHandler
from watchdog.observers import Observer
from S3 import S3Pipeline
from sqlalchemy import String, JSON, TIMESTAMP, func
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column 

class Base(DeclarativeBase):
    pass

class MetaData(Base):
    """
    Represents a table of uploaded metadata files

    Attributes:
        `filename` (str): Name of file
        `created` (datetime): Timestamp indicating when the entry was created.
        `updated` (datetime): Timestamp indicating the last time the entry was updated.
    """

    __tablename__ = "metadata_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String)
    content: Mapped[ty.Dict[str,ty.Any]] = mapped_column(JSON)
    created: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True),default=func.now())
    updated: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True),onupdate=func.now())


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


def parse_metadata(filename: str) -> ty.Union[int,ty.Dict[str,ty.Any]]:
    """
    Extract JSON data from metadata file 

    -Args:
        `filename`: String filename of metadata txt file

    -Returns:
        `content`: Decoded JSON object of experiment metadata
    
    """
    with open(filename,"r") as f:
        try:
            content = json.loads(f.read())
            print(f"Metadata Content is {content}")
            return content
      
        except json.JSONDecodeError as e:
            print(f"Error {e} File Content Not Properly Formatted")
            return None

class CustomHandler(LoggingEventHandler):
    """
    Custom class for our file event handler
    """

    def __init__(self,metadata_path: str, connection: Connection, S3_conn: S3Pipeline):
        super().__init__()
        self.metadata_path = metadata_path
        self.connection = connection
        self.S3_conn = S3_conn

    def on_created(self, event):
        path = event.src_path
        filename = os.path.basename(path)
        if not self.connection.entry_exists(filename):
            data = parse_metadata(path)
            if data:
                values = {"filename":filename, "content": data, 
                    "created":datetime.now(),"updated":datetime.now()
                }
                self.connection.insert_value(data)
                encoded_data = json.dumps(values, default=str)
                self.S3_conn.write_data(encoded_data,filename)
        else:
            print('File Already Added')


def main(directory: str) -> None:
    """
    Runs our event handler in the background which will parse all new metadata files added to specified directory

    -Args:
        directory: Name of directory where metadata files are to be found
    
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    load_dotenv('./.env')
    ACCESS_KEY= os.getenv('METADATA_ACCESS_KEY')
    SECRET_KEY= os.getenv('METADATA_SECRET_KEY')
    BUCKET_NAME = os.getenv('METADATA_BUCKET_NAME')
    SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')

    metadata_path = os.path.join(os.path.dirname(__file__),directory)
    S3_obj = S3Pipeline(ACCESS_KEY,SECRET_KEY, BUCKET_NAME, SESSION_TOKEN)

    sqlite_uri = f"sqlite:///{os.getcwd()}/metadata.db"

    if os.path.exists(metadata_path):
        connection = Connection(sqlite_uri,MetaData)
        path = metadata_path
        event_handler = CustomHandler(path, connection, S3_obj)
        observer = Observer()
        observer.schedule(event_handler, path, recursive=True)
        observer.start()
        try:
            while observer.is_alive():
                observer.join(1)
        finally:
            observer.stop()
            observer.join()
    else:
        print(f"Incorrect Metadata Path {metadata_path} Specified")

if __name__ == "__main__":
    #replace this with actual path as shown below
    path = os.path.join("C:", "FW", "WiWo", "metdata")
    main("metadata")



