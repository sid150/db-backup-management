#!/usr/bin/env python3
import os
import sys
import argparse
import datetime
from pathlib import Path
from typing import Optional, List
from manager import BackupManager
from cloud_store import backup_to_cloud

def setup_parser() -> argparse.ArgumentParser:
    """Set up command line argument parser."""
    parser = argparse.ArgumentParser(
        description='Database Backup Management CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Create a full backup
    cli.py backup full
    
    # Create an incremental backup
    cli.py backup incremental
    
    # List all backups
    cli.py list
    
    # Upload latest backup to cloud
    cli.py upload latest s3 --bucket my-backups
    
    # Restore from a backup file
    cli.py restore backups/full/backup_20250826.sql.gz
    
    # Create full backup and upload to S3
    cli.py backup full --upload s3 --bucket my-backups
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create a database backup')
    backup_parser.add_argument('type', choices=['full', 'incremental'], 
                             help='Type of backup to create')
    backup_parser.add_argument('--no-compress', action='store_true',
                             help='Do not compress the backup file')
    backup_parser.add_argument('--upload', choices=['s3', 'gcs', 'azure'],
                             help='Upload backup to cloud storage')
    backup_parser.add_argument('--bucket', help='Cloud storage bucket/container name')
    backup_parser.add_argument('--credentials', help='Path to cloud credentials file')
    
    # List command
    subparsers.add_parser('list', help='List all available backups')
    
    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload backup to cloud storage')
    upload_parser.add_argument('backup', choices=['latest', 'all'],
                             help='Which backup(s) to upload')
    upload_parser.add_argument('provider', choices=['s3', 'gcs', 'azure'],
                             help='Cloud storage provider')
    upload_parser.add_argument('--bucket', required=True,
                             help='Cloud storage bucket/container name')
    upload_parser.add_argument('--credentials', 
                             help='Path to cloud credentials file')
    
    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore from backup')
    restore_parser.add_argument('file', help='Backup file to restore from')
    
    return parser

def handle_backup(args: argparse.Namespace, manager: BackupManager) -> Optional[Path]:
    """Handle backup creation command."""
    try:
        compress = not args.no_compress
        if args.type == 'full':
            backup_file = manager.create_full_backup(compress=compress)
        else:
            backup_file = manager.create_incremental_backup(compress=compress)
        
        if args.upload and backup_file:
            cloud_args = {
                'bucket_name': args.bucket
            }
            
            # Add provider-specific arguments
            if args.credentials:
                if args.upload == 's3':
                    # For S3, expect AWS credentials file
                    with open(args.credentials) as f:
                        import json
                        creds = json.load(f)
                        cloud_args.update({
                            'aws_access_key_id': creds.get('aws_access_key_id'),
                            'aws_secret_access_key': creds.get('aws_secret_access_key'),
                            'region': creds.get('region', 'us-east-1')
                        })
                elif args.upload == 'gcs':
                    cloud_args['credentials_path'] = args.credentials
                elif args.upload == 'azure':
                    # For Azure, expect connection string in credentials file
                    with open(args.credentials) as f:
                        cloud_args['connection_string'] = f.read().strip()
            
            backup_to_cloud(str(backup_file), args.upload, **cloud_args)
        
        return backup_file
    except Exception as e:
        print(f"Error during backup: {str(e)}")
        return None

def handle_list(args: argparse.Namespace, manager: BackupManager) -> None:
    """Handle listing backups command."""
    try:
        full_backups, incr_backups = manager.list_backups()
        
        print("\nFull backups:")
        for backup in full_backups:
            size_mb = backup.stat().st_size / (1024 * 1024)
            print(f"- {backup.name} ({size_mb:.2f} MB)")
        
        print("\nIncremental backups:")
        for backup in incr_backups:
            size_mb = backup.stat().st_size / (1024 * 1024)
            print(f"- {backup.name} ({size_mb:.2f} MB)")
    except Exception as e:
        print(f"Error listing backups: {str(e)}")

def handle_upload(args: argparse.Namespace, manager: BackupManager) -> None:
    """Handle upload to cloud command."""
    try:
        full_backups, incr_backups = manager.list_backups()
        all_backups = sorted(full_backups + incr_backups,
                           key=lambda x: x.stat().st_mtime)
        
        if not all_backups:
            print("No backups found to upload")
            return
        
        cloud_args = {
            'bucket_name': args.bucket
        }
        
        # Add provider-specific arguments
        if args.credentials:
            if args.provider == 's3':
                with open(args.credentials) as f:
                    import json
                    creds = json.load(f)
                    cloud_args.update({
                        'aws_access_key_id': creds.get('aws_access_key_id'),
                        'aws_secret_access_key': creds.get('aws_secret_access_key'),
                        'region': creds.get('region', 'us-east-1')
                    })
            elif args.provider == 'gcs':
                cloud_args['credentials_path'] = args.credentials
            elif args.provider == 'azure':
                with open(args.credentials) as f:
                    cloud_args['connection_string'] = f.read().strip()
        
        if args.backup == 'latest':
            backup_files = [all_backups[-1]]
        else:  # 'all'
            backup_files = all_backups
        
        for backup_file in backup_files:
            print(f"\nUploading {backup_file.name}...")
            success = backup_to_cloud(str(backup_file), args.provider, **cloud_args)
            
            # Send notification about upload status
            try:
                from notifications import notify_backup_status
                backup_type = "full" if "full" in backup_file.name else "incremental"
                notify_backup_status(
                    backup_type=backup_type,
                    backup_file=backup_file,
                    success=True,
                    upload_status={args.provider.upper(): success}
                )
            except ImportError:
                print("Notifications module not available")
            except Exception as ne:
                print(f"Failed to send notification: {ne}")
            
    except Exception as e:
        print(f"Error during upload: {str(e)}")
        # Send failure notification
        try:
            from notifications import notify_backup_status
            backup_type = "full" if "full" in backup_file.name else "incremental"
            notify_backup_status(
                backup_type=backup_type,
                backup_file=backup_file,
                success=False,
                upload_status={args.provider.upper(): False}
            )
        except ImportError:
            print("Notifications module not available")
        except Exception as ne:
            print(f"Failed to send notification: {ne}")

def handle_restore(args: argparse.Namespace, manager: BackupManager) -> None:
    """Handle restore from backup command."""
    try:
        backup_path = Path(args.file)
        if not backup_path.exists():
            print(f"Backup file not found: {backup_path}")
            return
        
        manager.restore_backup(backup_path)
    except Exception as e:
        print(f"Error during restore: {str(e)}")

def main():
    parser = setup_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    manager = BackupManager('mysql')
    
    if args.command == 'backup':
        handle_backup(args, manager)
    elif args.command == 'list':
        handle_list(args, manager)
    elif args.command == 'upload':
        handle_upload(args, manager)
    elif args.command == 'restore':
        handle_restore(args, manager)

if __name__ == "__main__":
    main()
