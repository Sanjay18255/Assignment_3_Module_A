"""
db_manager.py — DatabaseManager for CallHub.
CS 432 Databases | Module A

Follows exact boilerplate structure:
  self.databases = {db_name: {table_name: Table}}

Return convention (matches main.ipynb usage):
  create_database → (True, msg)  or  (False, msg)
  delete_database → (True, msg)  or  (False, msg)
  list_databases  → [db_name, ...]
  create_table    → (True, msg)  or  (False, msg)
  delete_table    → (True, msg)  or  (False, msg)
  list_tables     → ([table_names], msg)  or  (None, msg)
  get_table       → (Table, msg)          or  (None, msg)
"""

from .table import Table
from .transaction import Transaction, WALLogger, crash_recovery, check_consistency


class DatabaseManager:
    def __init__(self):
        self.databases = {}   # {db_name: {table_name: Table instance}}

    #  Database DDL                                                        

    def create_database(self, db_name):
        """
        Create a new database with the given name.
        Initializes an empty dictionary for tables within this database.
        """
        if db_name in self.databases:
            return False, f"Database '{db_name}' already exists"
        self.databases[db_name] = {}
        return True, f"Database '{db_name}' created successfully"

    def delete_database(self, db_name):
        """
        Delete an existing database and all its tables.
        """
        if db_name not in self.databases:
            return False, f"Database '{db_name}' does not exist"
        del self.databases[db_name]
        return True, f"Database '{db_name}' deleted successfully"

    def list_databases(self):
        """
        Return a list of all database names currently managed.
        """
        return list(self.databases.keys())

    #  Table DDL                                                           
   
    def create_table(self, db_name, table_name, schema, order=8, search_key=None):
        """
        Create a new table within a specified database.
        - schema     : dictionary of column names and data types
        - order      : B+ tree order for indexing
        - search_key : field name to use as the key in the B+ Tree
        """
        if db_name not in self.databases:
            return False, f"Database '{db_name}' does not exist"
        if table_name in self.databases[db_name]:
            return False, f"Table '{table_name}' already exists in database '{db_name}'"

        self.databases[db_name][table_name] = Table(
            name=table_name,
            schema=schema,
            order=order,
            search_key=search_key
        )
        return True, f"Table '{table_name}' created successfully in database '{db_name}'"

    def delete_table(self, db_name, table_name):
        """
        Delete a table from the specified database.
        """
        if db_name not in self.databases:
            return False, f"Database '{db_name}' does not exist"
        if table_name not in self.databases[db_name]:
            return False, f"Table '{table_name}' does not exist in database '{db_name}'"
        del self.databases[db_name][table_name]
        return True, f"Table '{table_name}' deleted from database '{db_name}'"

    def list_tables(self, db_name):
        """
        List all tables within a given database.
        Returns ([table_names], message) or (None, error_message)
        """
        if db_name not in self.databases:
            return None, f"Database '{db_name}' does not exist"
        return list(self.databases[db_name].keys()), "OK"

    def get_table(self, db_name, table_name):
        """
        Retrieve a Table instance from a given database.
        Returns (Table, message) or (None, error_message)
        """
        if db_name not in self.databases:
            return None, f"Database '{db_name}' does not exist"
        if table_name not in self.databases[db_name]:
            return None, f"Table '{table_name}' does not exist in database '{db_name}'"
        return self.databases[db_name][table_name], "OK"

    #  Transaction support (Assignment 3)                                  
    def begin_transaction(self, db_name=None, log_file="callhub_wal.log"):
        """Begin a new ACID transaction. Returns a Transaction object."""
        wal = WALLogger(log_file)
        db_name = db_name or list(self.databases.keys())[0]
        return Transaction(self, wal, db_name)

    def recover(self, log_file="callhub_wal.log"):
        """Run crash recovery from the WAL log file."""
        wal = WALLogger(log_file)
        crash_recovery(self, wal)

    def check_consistency(self):
        """Check B+ Tree index is consistent with stored records."""
        return check_consistency(self)
