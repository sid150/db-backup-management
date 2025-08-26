import os
import sys
import gzip
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple
from utils import get_db_config

class RestoreManager:
    def __init__(self, db_type: str = 'mysql'):
        self.db_config = get_db_config(db_type)
        self.backup_dir = Path(__file__).parent.parent / "backups"
        self.full_backup_dir = self.backup_dir / "full"
        self.incremental_backup_dir = self.backup_dir / "incremental"

    def _decompress_file(self, compressed_path: Path, output_path: Optional[Path] = None) -> Path:
        """Decompress a gzip file to a temporary location."""
        if output_path is None:
            output_path = compressed_path.with_suffix('')
        
        with gzip.open(compressed_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return output_path

    def get_available_backups(self) -> Tuple[List[Path], List[Path]]:
        """Get lists of available full and incremental backups."""
        full_backups = sorted(
            self.full_backup_dir.glob("*.sql*"),
            key=lambda x: x.stat().st_mtime
        )
        incremental_backups = sorted(
            self.incremental_backup_dir.glob("*.sql*"),
            key=lambda x: x.stat().st_mtime
        )
        return full_backups, incremental_backups

    def restore_backup(self, backup_file: Path, temp_dir: Optional[Path] = None) -> None:
        """Restore a specific backup file."""
        if temp_dir is None:
            temp_dir = self.backup_dir / "temp"
            temp_dir.mkdir(exist_ok=True)

        try:

            if backup_file.suffix == '.gz':
                temp_file = temp_dir / f'temp_restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}.sql'
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

            print(f"Restoring from backup: {backup_file}")
            with open(backup_file, 'r') as f:
                subprocess.run(cmd, stdin=f, check=True)
            print(f"Backup restored successfully from: {backup_file}")

            if cleanup_needed:
                os.remove(backup_file)

        except subprocess.CalledProcessError as e:
            print(f"Error restoring backup: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error during restoration: {e}")
            raise

    def restore_to_point_in_time(self, target_datetime: datetime) -> None:
        """Restore database to a specific point in time using full + incremental backups."""
        full_backups, incremental_backups = self.get_available_backups()
        
        suitable_full_backup = None
        for backup in reversed(full_backups):
            backup_time = datetime.fromtimestamp(backup.stat().st_mtime)
            if backup_time <= target_datetime:
                suitable_full_backup = backup
                break
        
        if not suitable_full_backup:
            raise ValueError("No suitable full backup found before the target time")

        suitable_incremental_backups = []
        for backup in incremental_backups:
            backup_time = datetime.fromtimestamp(backup.stat().st_mtime)
            if backup_time > datetime.fromtimestamp(suitable_full_backup.stat().st_mtime) and backup_time <= target_datetime:
                suitable_incremental_backups.append(backup)

        temp_dir = self.backup_dir / "temp"
        temp_dir.mkdir(exist_ok=True)

        try:
            print(f"\nRestoring full backup: {suitable_full_backup}")
            self.restore_backup(suitable_full_backup, temp_dir)

            for backup in sorted(suitable_incremental_backups, key=lambda x: x.stat().st_mtime):
                print(f"\nRestoring incremental backup: {backup}")
                self.restore_backup(backup, temp_dir)

            print("\nRestoration completed successfully!")

        except Exception as e:
            print(f"\nError during restoration: {e}")
            raise
        finally:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

def list_available_backups(restore_manager: RestoreManager) -> None:
    """List all available backups with their timestamps."""
    full_backups, incremental_backups = restore_manager.get_available_backups()
    
    print("\nAvailable Backups:")
    print("\nFull Backups:")
    for backup in full_backups:
        timestamp = datetime.fromtimestamp(backup.stat().st_mtime)
        size_mb = backup.stat().st_size / (1024 * 1024)
        print(f"- {backup.name}")
        print(f"  Created: {timestamp}")
        print(f"  Size: {size_mb:.2f} MB")

    print("\nIncremental Backups:")
    for backup in incremental_backups:
        timestamp = datetime.fromtimestamp(backup.stat().st_mtime)
        size_mb = backup.stat().st_size / (1024 * 1024)
        print(f"- {backup.name}")
        print(f"  Created: {timestamp}")
        print(f"  Size: {size_mb:.2f} MB")

def main():
    restore_manager = RestoreManager()
    
    if len(sys.argv) < 2:
        print("Available commands:")
        print("  list              - List all available backups")
        print("  restore [file]    - Restore a specific backup file")
        print("  point [datetime]  - Restore to a specific point in time (format: YYYY-MM-DD HH:MM:SS)")
        sys.exit(1)

    command = sys.argv[1]

    try:
        if command == "list":
            list_available_backups(restore_manager)

        elif command == "restore" and len(sys.argv) == 3:
            backup_path = Path(sys.argv[2])
            if not backup_path.exists():
                print(f"Error: Backup file {backup_path} not found")
                sys.exit(1)
            restore_manager.restore_backup(backup_path)

        elif command == "point" and len(sys.argv) == 3:
            try:
                target_time = datetime.strptime(sys.argv[2], "%Y-%m-%d %H:%M:%S")
                restore_manager.restore_to_point_in_time(target_time)
            except ValueError:
                print("Error: Invalid datetime format. Use YYYY-MM-DD HH:MM:SS")
                sys.exit(1)

        else:
            print("Invalid command or missing arguments")
            sys.exit(1)

    except Exception as e:
        print(f"Restoration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
