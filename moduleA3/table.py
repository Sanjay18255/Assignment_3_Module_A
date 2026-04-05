"""
table.py — Table abstraction for CallHub, following instructor's boilerplate.
CS 432 Databases | Module A

Table(name, schema, order=8, search_key=None)
- schema : dict of {column_name: python_type}  e.g. {'member_id': int, 'name': str}
- search_key : which column is the B+ Tree index key

Return convention (matching boilerplate usage in main.ipynb):
  insert  → (True, key)      or  (False, error_msg)
  get     → record_dict      or  None
  update  → (True, msg)      or  (False, msg)
  delete  → (True, msg)      or  (False, msg)
  get_all → [(key, record), ...]
  range_query → [(key, record), ...]
"""

from .bplustree import BPlusTree


# Supported Python types for schema validation
TYPE_MAP = {
    'int':   int,
    'str':   str,
    'float': float,
    'bool':  bool,
    int:     int,
    str:     str,
    float:   float,
    bool:    bool,
}


class Table:
    def __init__(self, name, schema, order=8, search_key=None):
        self.name       = name        # Name of the table
        self.schema     = schema      # Table schema: dict of {column_name: data_type}
        self.order      = order       # Order of the B+ Tree (max number of children)
        self.data       = BPlusTree(order=order)   # Underlying B+ Tree
        self.search_key = search_key  # Primary / search key used for indexing

        # Derive the search key automatically if not given
        if self.search_key is None and schema:
            self.search_key = next(iter(schema))

    #  Validation                                                          

    def validate_record(self, record):
        """
        Validate that the given record matches the table schema:
        - All required columns are present
        - Data types are correct (coercion allowed for compatible types)
        Returns (True, None) or (False, error_message)
        """
        for col, expected_type in self.schema.items():
            if col not in record:
                return False, f"Missing required field: '{col}'"
            val = record[col]
            if val is None:
                continue   # allow NULLs
            actual_type = TYPE_MAP.get(expected_type, expected_type)
            if not isinstance(val, actual_type):
                # Try numeric coercion (int ↔ float)
                try:
                    actual_type(val)
                except (TypeError, ValueError):
                    return False, (
                        f"Field '{col}' expects {actual_type.__name__}, "
                        f"got {type(val).__name__}"
                    )
        # Example constraints (customize based on your schema)

        # No negative values for numeric fields
        for col, val in record.items():
            if isinstance(val, (int, float)) and val < 0:
                return False, f"Field '{col}' cannot be negative"
        return True, None

    
    #  CRUD                                                                

    def insert(self, record):
        """
        Insert a new record into the table.
        The record should be a dictionary matching the schema.
        The key used for insertion is the value of the `search_key` field.
        Returns (True, key) on success, (False, error_msg) on failure.
        """
        valid, err = self.validate_record(record)
        if not valid:
            return False, err

        if self.search_key not in record:
            return False, f"Search key '{self.search_key}' not found in record"

        key = record[self.search_key]
        self.data.insert(key, record)
        return True, key

    def get(self, record_id):
        """
        Retrieve a single record by its ID (i.e., the value of the search_key).
        Returns the record dict or None.
        """
        return self.data.search(record_id)

    def get_all(self):
        """
        Retrieve all records stored in the table in sorted order by search key.
        Returns [(key, record_dict), ...]
        """
        return self.data.get_all()

    def update(self, record_id, new_record):
        """
        Update a record identified by record_id with new_record data.
        Overwrites the existing entry.
        Returns (True, msg) or (False, msg).
        """
        existing = self.data.search(record_id)
        if existing is None:
            return False, f"Record with id '{record_id}' not found"

        # Merge updates into existing record
        updated = dict(existing)
        updated.update(new_record)

        # Ensure the search key stays consistent
        updated[self.search_key] = record_id

        valid, err = self.validate_record(updated)
        if not valid:
            return False, err

        self.data.update(record_id, updated)
        return True, "Record updated successfully"

    def delete(self, record_id):
        """
        Delete the record from the table by its record_id.
        Returns (True, 'Record deleted') or (False, 'Record not found').
        """
        result = self.data.delete(record_id)
        if result:
            return True, "Record deleted"
        return False, f"Record with id '{record_id}' not found"

    def range_query(self, start_value, end_value):
        """
        Perform a range query using the search key.
        Returns records where start_value <= key <= end_value.
        Returns [(key, record_dict), ...]
        """
        return self.data.range_query(start_value, end_value)

    
    #  Extra helpers (used in report/benchmarks)

    def search_by_field(self, field, value):
        """Full scan: return all records where record[field] == value."""
        return [(k, v) for k, v in self.data.get_all() if v.get(field) == value]

    def count(self):
        return self.data.count()

    def tree_height(self):
        return self.data.height()

    def __repr__(self):
        return (f"Table(name='{self.name}', "
                f"search_key='{self.search_key}', "
                f"records={self.count()}, "
                f"order={self.order})")
