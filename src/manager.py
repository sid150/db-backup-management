import os
import datetime
import subprocess
import gzip
import shutil
from pathlib import Path
from utils import get_db_config
from typing import Optional

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
    
    def _compress_file(self, file_path: Path) -> Path:
        """Compress a file using gzip compression."""
        compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        # Remove original uncompressed file
        os.remove(file_path)
        return compressed_path
    
    def _decompress_file(self, compressed_path: Path, output_path: Optional[Path] = None) -> Path:
        """Decompress a gzip file."""
        if output_path is None:
            output_path = compressed_path.with_suffix('')
        
        with gzip.open(compressed_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return output_path

    def _get_last_full_backup(self):
        full_backups = list(self.full_backup_dir.glob("*.sql*"))  # Match both .sql and .sql.gz
        if not full_backups:
            raise ValueError("No full backup found. Please create a full backup first.")
        return max(full_backups, key=lambda x: x.stat().st_mtime)
    
    def create_full_backup(self, compress: bool = True):
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
            
            if compress:
                backup_file = self._compress_file(backup_file)
                print(f"Backup compressed and saved as: {backup_file}")
            
            return backup_file
        except subprocess.CalledProcessError as e:
            print(f"Error creating full backup: {e}")
            raise
    
    def create_incremental_backup(self, compress: bool = True) -> Path:
        """Create an incremental backup based on binary logs since last full backup"""
        timestamp = self._get_timestamp()
        last_full_backup = self._get_last_full_backup()
        backup_file = self.incremental_backup_dir / f"incremental_backup_{timestamp}.sql"
        
        # If the last backup is compressed, decompress it temporarily to get its timestamp
        if last_full_backup.suffix == '.gz':
            temp_file = self.backup_dir / 'temp.sql'
            self._decompress_file(last_full_backup, temp_file)
            last_backup_time = datetime.datetime.fromtimestamp(temp_file.stat().st_mtime)
            os.remove(temp_file)
        else:
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
            
            if compress:
                backup_file = self._compress_file(backup_file)
                print(f"Backup compressed and saved as: {backup_file}")
            
            return backup_file
        except subprocess.CalledProcessError as e:
            print(f"Error creating incremental backup: {e}")
            raise

    def restore_backup(self, backup_file: Path) -> None:
        """Restore a backup file (can be either full or incremental)."""
        try:
            # If the backup is compressed, decompress it first
            if backup_file.suffix == '.gz':
                temp_file = self.backup_dir / 'temp_restore.sql'
                backup_file = self._decompress_file(backup_file, temp_file)
                cleanup_needed = True
            else:
                cleanup_needed = False

            cmd = [
                "mysql",
                "-h", self.db_config["host"],
                "-P", str(self.db_config["port"]),
                "-u", self.db_config["username"],
                f"--password={self.db_config['password']}",
                self.db_config["database"]
            ]

            with open(backup_file, 'r') as f:
                subprocess.run(cmd, stdin=f, check=True)
            print(f"Backup restored successfully from: {backup_file}")

            if cleanup_needed:
                os.remove(backup_file)  # Remove temporary decompressed file

        except subprocess.CalledProcessError as e:
            print(f"Error restoring backup: {e}")
            raise

    def list_backups(self) -> tuple[list[Path], list[Path]]:
        """List all available backups."""
        full_backups = sorted(
            self.full_backup_dir.glob("*.sql*"),
            key=lambda x: x.stat().st_mtime
        )
        incremental_backups = sorted(
            self.incremental_backup_dir.glob("*.sql*"),
            key=lambda x: x.stat().st_mtime
        )
        return full_backups, incremental_backups

def main():
    backup_manager = BackupManager('mysql')
    
    try:
        # Create a compressed full backup
        print("\nCreating full backup...")
        full_backup = backup_manager.create_full_backup(compress=True)
        print(f"Full backup created and compressed at: {full_backup}")
        
        # Create a compressed incremental backup
        print("\nCreating incremental backup...")
        incremental_backup = backup_manager.create_incremental_backup(compress=True)
        print(f"Incremental backup created and compressed at: {incremental_backup}")
        
        # List all backups
        print("\nListing all backups:")
        full_backups, incr_backups = backup_manager.list_backups()
        print("\nFull backups:")
        for backup in full_backups:
            size_mb = backup.stat().st_size / (1024 * 1024)
            print(f"- {backup.name} ({size_mb:.2f} MB)")
        
        print("\nIncremental backups:")
        for backup in incr_backups:
            size_mb = backup.stat().st_size / (1024 * 1024)
            print(f"- {backup.name} ({size_mb:.2f} MB)")
        
    except Exception as e:
        print(f"Backup operation failed: {e}")

if __name__ == "__main__":
    main()