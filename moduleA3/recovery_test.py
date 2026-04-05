from moduleA3.db_manager import DatabaseManager

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

db.create_table("CallHub", "Member_Role",
    {
        "id": int,
        "member_id": int,
        "role_id": int
    },
    search_key="id"
)

db.create_table("CallHub", "Department",
    {
        "department_id": int,
        "department_name": str
    },
    search_key="department_id"
)

print("\n=== RECOVERY RUN ===")
db.recover()

member, _ = db.get_table("CallHub", "Member")

print("Members after recovery:", member.get_all())