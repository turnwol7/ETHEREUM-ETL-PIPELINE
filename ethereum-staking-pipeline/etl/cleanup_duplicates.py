import os
import shutil
import datetime

def backup_and_remove_csv_files():
    """Backup and remove duplicate CSV files from the ETL directory"""
    # Get the current directory (ETL directory)
    etl_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create a backup directory with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = os.path.join(etl_dir, f"backup_{timestamp}")
    os.makedirs(backup_dir, exist_ok=True)
    
    # List of CSV files to backup and remove
    csv_files = [
        "staking_transactions.csv",
        "transformed_staking_data.csv",
        "daily_staking_stats.csv"
    ]
    
    # Backup and remove each file
    for filename in csv_files:
        file_path = os.path.join(etl_dir, filename)
        if os.path.exists(file_path):
            # Backup the file
            backup_path = os.path.join(backup_dir, filename)
            shutil.copy2(file_path, backup_path)
            print(f"Backed up {filename} to {backup_path}")
            
            # Remove the original file
            os.remove(file_path)
            print(f"Removed duplicate file: {file_path}")
        else:
            print(f"File {filename} not found in ETL directory")
    
    print(f"Backup completed. Files saved to {backup_dir}")
    print("The ETL pipeline will now use the CSV files from the root directory")

if __name__ == "__main__":
    backup_and_remove_csv_files() 