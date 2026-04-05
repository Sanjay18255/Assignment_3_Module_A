"""
transaction.py — Transaction Manager for CallHub
CS 432 Databases | Assignment 3 | Module A

Implements:
- ACID transaction support (Atomicity, Consistency, Durability)
- Write-Ahead Logging (WAL) for crash recovery
- Rollback on failure
- Consistency check between DB records and B+ Tree index
"""

import json
import os
import copy
import datetime
import threading

GLOBAL_LOCK = threading.Lock()


# WAL Log file path 
WAL_FILE = "callhub_wal.log"


def _timestamp():
	return datetime.datetime.now().isoformat()

# WAL Logger — Write-Ahead Log

class WALLogger:
	"""
	Write-Ahead Log (WAL) — every operation is logged BEFORE
	it is applied. On crash, uncommitted transactions are rolled back.

	Log entry format (JSON per line):
	{
	  "txn_id": 1,
	  "op":     "INSERT" | "UPDATE" | "DELETE",
	  "table":  "Member",
	  "key":    5,
	  "before": null | {old record},
	  "after":  {new record} | null,
	  "status": "BEGIN" | "COMMITTED" | "ROLLED_BACK",
	  "ts":     "2026-04-05T..."
	}
	"""

	def __init__(self, log_file=WAL_FILE):
		self.log_file = log_file

	def write(self, entry: dict):
		with open(self.log_file, "a") as f:
			f.write(json.dumps(entry) + "\n")

	def read_all(self):
		if not os.path.exists(self.log_file):
			return []
		with open(self.log_file, "r") as f:
			entries = []
			for line in f:
				line = line.strip()
				if line:
					try:
						entries.append(json.loads(line))
					except json.JSONDecodeError:
						pass
		return entries

	def clear(self):
		if os.path.exists(self.log_file):
			os.remove(self.log_file)

	def get_uncommitted(self):
		"""Return all txn_ids that started but never committed or rolled back."""
		entries = self.read_all()
		begun, done = set(), set()
		for e in entries:
			if e.get("status") == "BEGIN":
				begun.add(e["txn_id"])
			elif e.get("status") in ("COMMITTED", "ROLLED_BACK"):
				done.add(e["txn_id"])
		return begun - done

def fk_exists(db_manager, db_name, table_name, value):
    table, _ = db_manager.get_table(db_name, table_name)
    return table.get(value) is not None


# Transaction

class Transaction:
	"""
	Represents a single transaction.

	Usage:
		txn = db_manager.begin_transaction()
		try:
			txn.insert("Member", record)
			txn.update("Member", 3, {"primary_phone": "9999"})
			txn.commit()
		except Exception as e:
			txn.rollback()
	"""

	_id_counter = 0

	def __init__(self, db_manager, wal: WALLogger, db_name: str = None):
		Transaction._id_counter += 1
		self.txn_id     = Transaction._id_counter
		self.db_manager = db_manager
		self.wal        = wal
		self.ops        = []      # list of (op, table, key, before, after)
		self.committed  = False
		self.rolled_back= False
		self.db_name    = db_name or list(db_manager.databases.keys())[0]

		self.wal.write({
			"txn_id": self.txn_id,
			"op":     "BEGIN",
			"status": "BEGIN",
			"ts":     _timestamp()
		})
		print(f"  [TXN {self.txn_id}] BEGIN")

	#Operations

	def insert(self, table_name: str, record: dict):
		with GLOBAL_LOCK:
			table, _ = self.db_manager.get_table(self.db_name, table_name)
			#  FOREIGN KEY CHECKS

			# Member → Department
			if table_name == "Member":
				if not fk_exists(self.db_manager, self.db_name, "Department", record.get("department_id")):
					raise RuntimeError("FK violation: department_id does not exist")

			# Department → Member (HOD)
			# allow NULL OR defer check if Member not yet inserted
			if table_name == "Department" and record.get("hod_member_id") is not None:
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("hod_member_id")):
					print("⚠ Warning: HOD member not yet present (possible circular dependency)")
			# Member_Role
			if table_name == "Member_Role":
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("member_id")):
					raise RuntimeError("FK violation: member_id invalid")
				if not fk_exists(self.db_manager, self.db_name, "Role", record.get("role_id")):
					raise RuntimeError("FK violation: role_id invalid")

			# Member_Contact
			if table_name == "Member_Contact":
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("member_id")):
					raise RuntimeError("FK violation: member_id invalid")

			# Hostel
			if table_name == "Hostel" and record.get("caretaker_member_id") is not None:
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("caretaker_member_id")):
					raise RuntimeError("FK violation: caretaker_member_id invalid")

			# Lab
			if table_name == "Lab":
				if not fk_exists(self.db_manager, self.db_name, "Department", record.get("department_id")):
					raise RuntimeError("FK violation: department_id invalid")
				if record.get("incharge_member_id") is not None:
					if not fk_exists(self.db_manager, self.db_name, "Member", record.get("incharge_member_id")):
						raise RuntimeError("FK violation: incharge_member_id invalid")

			# Office_Room
			if table_name == "Office_Room":
				if not fk_exists(self.db_manager, self.db_name, "Department", record.get("department_id")):
					raise RuntimeError("FK violation: department_id invalid")

			# Directory_Interaction_Log
			if table_name == "Directory_Interaction_Log":
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("actor_member_id")):
					raise RuntimeError("FK violation: actor_member_id invalid")
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("target_member_id")):
					raise RuntimeError("FK violation: target_member_id invalid")

			# Role_Permission
			if table_name == "Role_Permission":
				if not fk_exists(self.db_manager, self.db_name, "Role", record.get("role_id")):
					raise RuntimeError("FK violation: role_id invalid")
				if not fk_exists(self.db_manager, self.db_name, "Permission", record.get("permission_id")):
					raise RuntimeError("FK violation: permission_id invalid")

			# Search_Log
			if table_name == "Search_Log":
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("member_id")):
					raise RuntimeError("FK violation: member_id invalid")
				if record.get("filter_department_id") is not None:
					if not fk_exists(self.db_manager, self.db_name, "Department", record.get("filter_department_id")):
						raise RuntimeError("FK violation: filter_department_id invalid")
				if record.get("filter_role_id") is not None:
					if not fk_exists(self.db_manager, self.db_name, "Role", record.get("filter_role_id")):
						raise RuntimeError("FK violation: filter_role_id invalid")

			# Login_History
			if table_name == "Login_History":
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("member_id")):
					raise RuntimeError("FK violation: member_id invalid")

			# Audit_Log
			if table_name == "Audit_Log":
				if not fk_exists(self.db_manager, self.db_name, "Member", record.get("performed_by_member_id")):
					raise RuntimeError("FK violation: performed_by_member_id invalid")
				if record.get("target_member_id") is not None:
					if not fk_exists(self.db_manager, self.db_name, "Member", record.get("target_member_id")):
						raise RuntimeError("FK violation: target_member_id invalid")
			key = record.get(table.search_key)
			self.wal.write({
				"txn_id": self.txn_id, "op": "INSERT",
				"table": table_name, "key": key,
				"before": None, "after": copy.deepcopy(record),
				"status": "PENDING", "ts": _timestamp()
			})

			ok, result = table.insert(record)
			if not ok:
				raise RuntimeError(f"INSERT failed: {result}")

			self.ops.append(("INSERT", table_name, key, None, copy.deepcopy(record)))

	def update(self, table_name: str, key, updates: dict):
		with GLOBAL_LOCK:
			table, _ = self.db_manager.get_table(self.db_name, table_name)
			before = copy.deepcopy(table.get(key))
			if before is None:
				raise RuntimeError(f"UPDATE failed: key {key} not found in {table_name}")

			self.wal.write({
				"txn_id": self.txn_id, "op": "UPDATE",
				"table": table_name, "key": key,
				"before": before, "after": updates,
				"status": "PENDING", "ts": _timestamp()
			})

			ok, msg = table.update(key, updates)
			if not ok:
				raise RuntimeError(f"UPDATE failed: {msg}")

			after = copy.deepcopy(table.get(key))
			self.ops.append(("UPDATE", table_name, key, before, after))

	def delete(self, table_name: str, key):
		with GLOBAL_LOCK:
			table, _ = self.db_manager.get_table(self.db_name, table_name)
			before = copy.deepcopy(table.get(key))
			if before is None:
				raise RuntimeError(f"DELETE failed: key {key} not found in {table_name}")

			self.wal.write({
				"txn_id": self.txn_id, "op": "DELETE",
				"table": table_name, "key": key,
				"before": before, "after": None,
				"status": "PENDING", "ts": _timestamp()
			})

			ok, msg = table.delete(key)
			if not ok:
				raise RuntimeError(f"DELETE failed: {msg}")

			self.ops.append(("DELETE", table_name, key, before, None))

	# Commit

	def commit(self):
		"""Commit the transaction — mark it as durable in the WAL."""
		if self.committed or self.rolled_back:
			return
		self.committed = True
		self.wal.write({
			"txn_id": self.txn_id,
			"op":     "COMMIT",
			"status": "COMMITTED",
			"ts":     _timestamp()
		})
		print(f"  [TXN {self.txn_id}] COMMITTED ({len(self.ops)} ops)")

	# Rollback

	def rollback(self):
		"""
		Rollback all operations in REVERSE order.
		Atomicity: if anything fails, undo everything.
		"""
		if self.committed or self.rolled_back:
			return
		self.rolled_back = True

		print(f"  [TXN {self.txn_id}] ROLLING BACK {len(self.ops)} ops...")

		for op, table_name, key, before, after in reversed(self.ops):
			table, _ = self.db_manager.get_table(self.db_name, table_name)
			try:
				if op == "INSERT":
					# Undo insert → delete
					table.delete(key)
					print(f"    ↩ Undo INSERT: deleted {table_name}[{key}]")
				elif op == "UPDATE":
					# Undo update → restore before state
					table.update(key, before)
					print(f"    ↩ Undo UPDATE: restored {table_name}[{key}]")
				elif op == "DELETE":
					# Undo delete → re-insert
					table.insert(before)
					print(f"    ↩ Undo DELETE: re-inserted {table_name}[{key}]")
			except Exception as e:
				print(f"    ⚠ Rollback error on {op} {table_name}[{key}]: {e}")

		self.wal.write({
			"txn_id": self.txn_id,
			"op":     "ROLLBACK",
			"status": "ROLLED_BACK",
			"ts":     _timestamp()
		})
		print(f"  [TXN {self.txn_id}] ROLLED BACK ↩")


# Crash Recovery
def crash_recovery(db_manager, wal: WALLogger):
	print("\n CRASH RECOVERY — Scanning WAL...")
	entries = wal.read_all()

	committed = set()
	uncommitted = wal.get_uncommitted()

	# find committed txns
	for e in entries:
		if e.get("status") == "COMMITTED":
			committed.add(e["txn_id"])

	# REDO committed
	print("\n REDO committed transactions...")
	for e in entries:
		if e.get("txn_id") in committed and e.get("op") in ("INSERT", "UPDATE", "DELETE"):
			table, _ = db_manager.get_table(list(db_manager.databases.keys())[0], e["table"])

			if table is None:
				print(f"⚠ Table {e['table']} not found during REDO, skipping...")
				continue

			if e["op"] == "INSERT":
				table.insert(e["after"])
			elif e["op"] == "UPDATE":
				table.update(e["key"], e["after"])
			elif e["op"] == "DELETE":
				table.delete(e["key"])

	# UNDO uncommitted
	print("\n↩ UNDO uncommitted transactions...")
	txn_ops = {}

	for e in entries:
		tid = e.get("txn_id")
		if tid in uncommitted and e.get("op") in ("INSERT", "UPDATE", "DELETE"):
			txn_ops.setdefault(tid, []).append(e)

	for tid, ops in txn_ops.items():
		for entry in reversed(ops):
			table, _ = db_manager.get_table(list(db_manager.databases.keys())[0], entry["table"])

			if table is None:
				print(f"⚠ Table {entry['table']} not found during UNDO, skipping...")
				continue

			if entry["op"] == "INSERT":
				table.delete(entry["key"])
			elif entry["op"] == "UPDATE":
				table.update(entry["key"], entry["before"])
			elif entry["op"] == "DELETE":
				table.insert(entry["before"])

	print("\n Recovery complete.")

# Consistency Checker

def check_consistency(db_manager):
	"""
	Verify that the B+ Tree index is consistent with the stored records.
	Every record in the tree must be retrievable by its key.
	Returns (is_consistent: bool, issues: list)
	"""
	issues = []
	for db_name, tables in db_manager.databases.items():
		for tname, table in tables.items():
			all_records = table.get_all()
			for key, record in all_records:
				# Verify the key in the record matches the B+ Tree key
				rec_key = record.get(table.search_key)
				if rec_key != key:
					issues.append(
						f"{db_name}.{tname}: key mismatch — "
						f"tree key={key}, record key={rec_key}"
					)
				# Verify the record is searchable
				found = table.get(key)
				if found is None:
					issues.append(
						f"{db_name}.{tname}: key {key} in get_all() "
						f"but not searchable via get()"
					)

	return len(issues) == 0, issues
