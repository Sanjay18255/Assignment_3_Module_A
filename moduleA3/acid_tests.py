from moduleA3.db_manager import DatabaseManager
import threading

# SETUP

def setup_db():
    db = DatabaseManager()
    db.create_database("CallHub")

    db.create_table("CallHub", "Member",
        {
            "member_id": int,
            "member_name": str,
            "department_id": int
        },
        search_key="member_id"
    )

    db.create_table("CallHub", "Department",
        {
            "department_id": int,
            "department_name": str
        },
        search_key="department_id"
    )

    # Insert base department
    txn = db.begin_transaction("CallHub")
    txn.insert("Department", {
        "department_id": 1,
        "department_name": "CSE"
    })
    txn.commit()

    return db


# ATOMICITY 

def test_atomicity(db):
    print("\n=== ATOMICITY TEST ===")

    txn = db.begin_transaction("CallHub")

    try:
        txn.insert("Member", {
            "member_id": 200,
            "member_name": "Invalid",
            "department_id": 999   # invalid FK
        })
        txn.commit()
    except Exception as e:
        print("Rollback triggered:", e)
        txn.rollback()

    member, _ = db.get_table("CallHub", "Member")
    print("Atomicity (should be False):", member.get(200) is not None)


# CONSISTENCY 

def test_consistency(db):
    print("\n=== CONSISTENCY TEST ===")

    ok, issues = db.check_consistency()
    print("Consistency:", ok)
    print("Issues:", issues)


# ISOLATION 

def test_isolation(db):
    print("\n=== ISOLATION TEST ===")

    def worker(i):
        txn = db.begin_transaction("CallHub")
        txn.insert("Member", {
            "member_id": 300 + i,
            "member_name": f"User{i}",
            "department_id": 1
        })
        txn.commit()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("Isolation test complete")


# DURABILITY 

def test_durability(db):
    print("\n=== DURABILITY TEST ===")

    txn = db.begin_transaction("CallHub")
    txn.insert("Member", {
        "member_id": 500,
        "member_name": "Durable",
        "department_id": 1
    })
    txn.commit()

    print("Simulate crash → run recovery_test.py separately")
    print("After recovery, record should still exist")


# STRESS TEST 

def test_stress(db):
    print("\n=== STRESS TEST ===")

    for i in range(50):
        txn = db.begin_transaction("CallHub")
        txn.insert("Member", {
            "member_id": 600 + i,
            "member_name": f"Stress{i}",
            "department_id": 1
        })
        txn.commit()

    print("Stress test complete")


# MAIN 

if __name__ == "__main__":
    db = setup_db()

    test_atomicity(db)
    test_consistency(db)
    test_isolation(db)
    test_stress(db)
    test_durability(db)

    # Final consistency check
    print("\n=== FINAL CONSISTENCY CHECK ===")
    ok, issues = db.check_consistency()
    print("Final consistency:", ok)
    print("Issues:", issues)


def test_race_condition(db):
    print("\n=== RACE CONDITION TEST ===")

    def worker():
        txn = db.begin_transaction("CallHub")
        try:
            txn.insert("Member", {
                "member_id": 999,   # SAME ID
                "member_name": "Race",
                "department_id": 1
            })
            txn.commit()
        except:
            txn.rollback()

    threads = [threading.Thread(target=worker) for _ in range(5)]

    for t in threads: t.start()
    for t in threads: t.join()

    member, _ = db.get_table("CallHub", "Member")
    print("Race condition result:", member.get_all())