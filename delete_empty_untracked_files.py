import os

# List of empty, untracked files to delete
files_to_delete = [
    "check_audit_structure.py",
    "check_fcl_lcl_logic.py",
    "check_invoice_D1987058.py",
    "check_lcl_breakdown.py",
    "check_template_mappings.py"
]

for filename in files_to_delete:
    if os.path.exists(filename):
        if os.path.getsize(filename) == 0:
            os.remove(filename)
            print(f"Deleted empty file: {filename}")
        else:
            print(f"Skipped non-empty file: {filename}")
    else:
        print(f"File not found: {filename}")
        
            