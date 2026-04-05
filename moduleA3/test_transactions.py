from .db_manager import DatabaseManager
# SETUP

db = DatabaseManager()
db.create_database("callhub")

# Create ONLY required subset of tables for demo
# (you don't need all 14, just enough to prove FK + multi-table)

db.create_table("callhub", "Department", {
    "department_id": int,
    "department_name": str
}, search_key="department_id")

db.create_table("callhub", "Member", {
    "member_id": int,
    "member_name": str,
    "department_id": int
}, search_key="member_id")

db.create_table("callhub", "Role", {
    "role_id": int,
    "role_name": str
}, search_key="role_id")

db.create_table("callhub", "Member_Role", {
    "member_id": int,
    "role_id": int,
    "start_date": str
}, search_key="member_id")  # simplified key

# RECOVERY
print("\n=== RECOVERY PHASE ===")
db.recover()

# INITIAL DATA

dept, _ = db.get_table("callhub", "Department")
member, _ = db.get_table("callhub", "Member")
role, _ = db.get_table("callhub", "Role")

dept.insert({"department_id": 1, "department_name": "CSE"})
role.insert({"role_id": 10, "role_name": "Student"})

# TEST 1: FK VIOLATION

print("\n=== TEST 1: FK VIOLATION ===")

txn = db.begin_transaction("callhub")

try:
    txn.insert("Member", {
        "member_id": 1,
        "member_name": "Thushar",
        "department_id": 999   # invalid FK
    })
    txn.commit()
except Exception as e:
    print("Expected FK failure:", e)
    txn.rollback()

print("Consistency:", db.check_consistency())

# TEST 2: VALID MULTI-TABLE TRANSACTION

print("\n=== TEST 2: VALID MULTI-TABLE TRANSACTION ===")

txn = db.begin_transaction("callhub")

try:
    txn.insert("Member", {
        "member_id": 1,
        "member_name": "Thushar",
        "department_id": 1
    })

    txn.insert("Member_Role", {
        "member_id": 1,
        "role_id": 10,
        "start_date": "2024-01-01"
    })

    txn.commit()
except Exception as e:
    print("Error:", e)
    txn.rollback()

print("Consistency:", db.check_consistency())

print("Members:", member.get_all())

# TEST 3: ROLLBACK

print("\n=== TEST 3: FORCED ROLLBACK ===")

txn = db.begin_transaction("callhub")

try:
    txn.insert("Member", {
        "member_id": 2,
        "member_name": "FailCase",
        "department_id": 1
    })

    # force failure
    raise Exception("Simulated failure")

    txn.commit()

except Exception as e:
    print("Rollback triggered:", e)
    txn.rollback()

print("Members after rollback:", member.get_all())
print("Consistency:", db.check_consistency())

# TEST 4: CRASH SIMULATION

print("\n=== TEST 4: CRASH SIMULATION ===")

txn = db.begin_transaction("callhub")

txn.insert("Member", {
    "member_id": 3,
    "member_name": "CrashUser",
    "department_id": 1
})

txn.insert("Member_Role", {
    "member_id": 3,
    "role_id": 10,
    "start_date": "2024-01-01"
})

print("⚠ Simulating crash BEFORE commit...")
raise Exception("CRASH")