import sqlalchemy as sa
from utils import get_db_config
from sqlalchemy.orm import Session, DeclarativeBase, mapped_column
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import insert
import datetime

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = 'users'
    id = mapped_column(Integer, primary_key = True, autoincrement=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    def __repr__(self):
        return f"<User(id={self.id}, name={self.name}, email={self.email}, created_at={self.created_at})>"

class Address(Base):
    __tablename__ = 'addresses'
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, sa.ForeignKey('users.id'), nullable=False)
    address = Column(String(200), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    def __repr__(self):
        return f"<Address(id={self.id}, user_id={self.user_id}, address={self.address}, created_at={self.created_at})>"
    

def create_tables(db_type: str) -> sa.Engine:
    db_config = get_db_config(db_type)
    engine = sa.engine_from_config(db_config, prefix=f"{db_type}.")
    
    Base.metadata.create_all(engine)
    print(f"Database and tables created successfully for {db_type}.")
    return engine

def add_users(engine: sa.Engine, users_data: list[dict]) -> None:
    with Session(engine) as session:
        try:
            users = [User(**user_data) for user_data in users_data]
            session.add_all(users)
            
            # Commit the transaction
            session.commit()
            print(f"Successfully inserted {len(users)} users")
            for user in users:
                print(f"Inserted user: {user}")
        except Exception as e:
            session.rollback()
            print(f"Error occurred: {str(e)}")
            raise

def main():
    try:
        users_data = [
            {"name": "John Doe", "email": "john.doe@abc.com"},
            {"name": "Jane Smith", "email": "jane.smith@abc.com"},
            {"name": "Bob Johnson", "email": "bob.johnson@abc.com"},
            {"name": "Alice Brown", "email": "alice.brown@abc.com"},
            {"name": "Charlie Wilson", "email": "charlie.wilson@abc.com"}
        ]
        
        engine = create_tables("mysql")
        add_users(engine, users_data)
    except Exception as e:
        print(f"Failed to execute: {str(e)}")

if __name__ == "__main__":
    main()