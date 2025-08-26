import os
import datetime
import subprocess
from pathlib import Path
from utils import get_db_config

class BackupManager:
    def __init__(self, db_type):
        self.db_config = get_db_config(db_type)
        self.backup_dir = Path(__file__).parent.parent / "backups"
        self.backup_dir.mkdir(exist_ok=True)
        self.full_backup_dir = self.backup_dir / "full"
        self.incremental_backup_dir = self.backup_dir / "incremental"
        self.full_backup_dir.mkdir(exist_ok=True)
        self.incremental_backup_dir.mkdir(exist_ok=True)
    
    def _get_timestamp(self):
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    

    def _get_last_full_backup(self):
        full_backups = list(self.full_backup_dir.glob("*.sql"))
        if not full_backups:
            raise ValueError("No full backup found. Please create a full backup first.")
        return max(full_backups, key=lambda x: x.stat().st_mtime)
    
    def create_full_backup(self):
        timestamp = self._get_timestamp()
        backup_file = self.full_backup_dir / f"full_backup_{timestamp}.sql"
        
        cmd = [
            "mysqldump",
            "-h", self.db_config["host"],
            "-P", str(self.db_config["port"]),
            "-u", self.db_config["username"],
            f"--password={self.db_config['password']}",
            "--single-transaction",  
            "--routines",  
            "--triggers",  
            "--events",    
            self.db_config["database"]
        ]
        
        try:
            with open(backup_file, 'w') as f:
                subprocess.run(cmd, stdout=f, check=True)
            print(f"Full backup created successfully: {backup_file}")
            return backup_file
        except subprocess.CalledProcessError as e:
            print(f"Error creating full backup: {e}")
            raise
    
    def create_incremental_backup(self) -> Path:
        """Create an incremental backup based on binary logs since last full backup"""
        timestamp = self._get_timestamp()
        last_full_backup = self._get_last_full_backup()
        backup_file = self.incremental_backup_dir / f"incremental_backup_{timestamp}.sql"
        
        last_backup_time = datetime.datetime.fromtimestamp(last_full_backup.stat().st_mtime)
        formatted_time = last_backup_time.strftime("%Y-%m-%d %H:%M:%S")
        
        cmd = [
            "mysqldump",
            "-h", self.db_config["host"],
            "-P", str(self.db_config["port"]),
            "-u", self.db_config["username"],
            f"--password={self.db_config['password']}",
            "--single-transaction",
            "--insert-ignore", 
            f"--where='created_at > \"{formatted_time}\"'",  
            self.db_config["database"]
        ]
        
        try:
            with open(backup_file, 'w') as f:
                subprocess.run(cmd, stdout=f, check=True)
            print(f"Incremental backup created successfully: {backup_file}")
            return backup_file
        except subprocess.CalledProcessError as e:
            print(f"Error creating incremental backup: {e}")
            raise
    

def main():
    backup_manager = BackupManager('mysql')
    
    try:
        full_backup = backup_manager.create_full_backup()
        print(f"Full backup created at: {full_backup}")
        
        incremental_backup = backup_manager.create_incremental_backup()
        print(f"Incremental backup created at: {incremental_backup}")
        
    except Exception as e:
        print(f"Backup operation failed: {e}")

if __name__ == "__main__":
    main()