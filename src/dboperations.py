import sqlalchemy as sa
import subprocess
import os
import gzip
from datetime import datetime
from pathlib import Path
from utils import get_db_config
from sqlalchemy.orm import Session, DeclarativeBase, mapped_column
from sqlalchemy import Column, Integer, String, DateTime, text
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
    

def get_engine(db_type: str = "mysql") -> sa.Engine:
    """Create and return a SQLAlchemy engine."""
    db_config = get_db_config(db_type)
    if not db_config:
        raise ValueError(f"Failed to get database configuration for {db_type}")
    
    engine = sa.engine_from_config(db_config, prefix=f"{db_type}.")
    return engine

def create_tables(engine: sa.Engine) -> None:
    """Create all database tables."""
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully.")

def create_backup_directories():
    """Create backup directories if they don't exist."""
    config = get_db_config("mysql")
    base_path = Path(config.get("backup", {}).get("local_backup_path", "./backups"))
    
    full_backup_path = base_path / "full"
    incremental_backup_path = base_path / "incremental"
    
    full_backup_path.mkdir(parents=True, exist_ok=True)
    incremental_backup_path.mkdir(parents=True, exist_ok=True)
    
    return full_backup_path, incremental_backup_path

def create_full_backup() -> str:
    """Create a full backup of the database."""
    config = get_db_config("mysql")
    full_backup_path, _ = create_backup_directories()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = full_backup_path / f"full_backup_{timestamp}.sql"
    
    mysqldump_cmd = [
        "mysqldump",
        f"--host={config['host']}",
        f"--port={config['port']}",
        f"--user={config['username']}",
        f"--password={config['password']}",
        "--single-transaction",
        "--routines",
        "--triggers",
        "--events",
        config["database"]
    ]
    
    try:
        with open(backup_file, 'w') as f:
            subprocess.run(mysqldump_cmd, stdout=f, check=True)
        
        if config.get("backup", {}).get("compression", True):
            with open(backup_file, 'rb') as f_in:
                with gzip.open(f"{backup_file}.gz", 'wb') as f_out:
                    f_out.writelines(f_in)
            os.remove(backup_file)  
            backup_file = f"{backup_file}.gz"
        
        logger.info(f"Full backup created successfully: {backup_file}")
        return str(backup_file)
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create backup: {e}")
        raise

def create_incremental_backup(since_datetime: datetime = None) -> str:
    """Create an incremental backup of the database since the given datetime."""
    config = get_db_config("mysql")
    _, incremental_backup_path = create_backup_directories()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = incremental_backup_path / f"incremental_backup_{timestamp}.sql"
    
    mysqldump_cmd = [
        "mysqldump",
        f"--host={config['host']}",
        f"--port={config['port']}",
        f"--user={config['username']}",
        f"--password={config['password']}",
        "--single-transaction",
        "--routines",
        "--triggers",
        "--events",
        "--skip-add-locks"
    ]
    
    if since_datetime:
        where_condition = f"created_at >= '{since_datetime.strftime('%Y-%m-%d %H:%M:%S')}'"
        mysqldump_cmd.extend([
            "--where=" + where_condition
        ])
    
    mysqldump_cmd.append(config["database"])
    
    try:
   
        with open(backup_file, 'w') as f:
            subprocess.run(mysqldump_cmd, stdout=f, check=True)
        
        if config.get("backup", {}).get("compression", True):
            with open(backup_file, 'rb') as f_in:
                with gzip.open(f"{backup_file}.gz", 'wb') as f_out:
                    f_out.writelines(f_in)
            os.remove(backup_file)  
            backup_file = f"{backup_file}.gz"
        
        logger.info(f"Incremental backup created successfully: {backup_file}")
        return str(backup_file)
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create incremental backup: {e}")
        raise

def get_last_backup_time() -> datetime:
    """Get the timestamp of the last successful backup."""
    engine = get_engine()
    
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT MAX(created_at) as last_backup FROM ("
            "SELECT created_at FROM users UNION ALL "
            "SELECT created_at FROM addresses) as combined"
        )).fetchone()
        
        return result[0] if result and result[0] else None

def add_users(session: Session, users_data: list[dict]) -> list[User]:
    try:
        users = [User(**user_data) for user_data in users_data]
        session.add_all(users)
        print(f"Successfully inserted {len(users)} users")
        for user in users:
            print(f"Inserted user: {user}")
        return users
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

def add_addresses(session: Session, addresses_data: list[dict]) -> None:
    try:
        addresses = [Address(**addr_data) for addr_data in addresses_data]

        session.add_all(addresses)
        print(f"Successfully inserted {len(addresses)} addresses")
        for addr in addresses:
            print(f"Inserted address: {addr}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

def initialize_database():
    """Initialize the database and create sample data."""
    try:
        engine = get_engine()
        create_tables(engine)
        
        users_data = [
            {"name": "John Doe", "email": "john.doe@abc.com"},
            {"name": "Jane Smith", "email": "jane.smith@abc.com"},
            {"name": "Bob Johnson", "email": "bob.johnson@abc.com"}
        ]
        
        with Session(engine) as session:
            try:
                users = add_users(session, users_data)
                session.flush()
                
                addresses_data = [
                    {"user_id": users[0].id, "address": "123 Main St, City1, Country"},
                    {"user_id": users[1].id, "address": "456 Second St, City2, Country"},
                    {"user_id": users[2].id, "address": "789 Park Ave, City3, Country"}
                ]
                
                add_addresses(session, addresses_data)
                session.commit()
                logger.info("Database initialized with sample data")

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to initialize database: {e}")
                raise
                
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def main():
    """Main function to test backup functionality."""
    try:
        initialize_database()
    
        full_backup_path = create_full_backup()
        logger.info(f"Full backup created at: {full_backup_path}")
        
        last_backup_time = get_last_backup_time()
        
        if last_backup_time:
            incremental_backup_path = create_incremental_backup(last_backup_time)
            logger.info(f"Incremental backup created at: {incremental_backup_path}")
        
    except Exception as e:
        logger.error(f"Backup operation failed: {e}")
        raise

if __name__ == "__main__":
    main()