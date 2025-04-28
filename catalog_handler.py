from dataclasses import dataclass
import json
from typing import Optional
import os

from constants import CATALOG_LOCATION

@dataclass
class TableSnapshot:
    table_name: str
    latest_snapshot_id: str

class JSONFileHandler:
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    def read(self) -> Optional[TableSnapshot]:
        """Read JSON data from file and return as TableSnapshot object."""
        if not os.path.exists(self.file_path):
            return None
            
        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)
                return TableSnapshot(
                    table_name=data.get('table_name', ''),
                    latest_snapshot_id=data.get('latest_snapshot_id', '')
                )
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error reading file: {e}")
            return None
    
    def write(self, table_snapshot: TableSnapshot) -> bool:
        """Write TableSnapshot object to JSON file."""
        try:
            data = {
                'table_name': table_snapshot.table_name,
                'latest_snapshot_id': table_snapshot.latest_snapshot_id
            }
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            
            with open(self.file_path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            print(f"Error writing file: {e}")
            return False



# Example usage
if __name__ == "__main__":
    # Create a handler for our JSON file
    handler = JSONFileHandler(CATALOG_LOCATION)
    
    # Create a new TableSnapshot object
    snapshot = TableSnapshot(
        table_name="customers",
        latest_snapshot_id="snap-20250411-001"
    )
    
    # Write to file
    if handler.write(snapshot):
        print(f"Successfully wrote snapshot to {handler.file_path}")
    
    # Read from file
    loaded_snapshot = handler.read()
    if loaded_snapshot:
        print(f"Loaded snapshot: {loaded_snapshot}")