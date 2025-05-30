import os

# function to clean up log files 
def delete_all_logs(log_root="logs"):
    deleted_files = []

    for root, dirs, files in os.walk(log_root):
        for file in files:
            if file.endswith(".log"):
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    deleted_files.append(file_path)
                except Exception as e:
                    print(f"❌ Failed to delete {file_path}: {e}")

    if deleted_files:
        print(f"🧹 Deleted {len(deleted_files)} .log file(s):")
        for path in deleted_files:
            print(f"  - {path}")
    else:
        print("✅ Cleanup complete. No .log files found.")

if __name__ == "__main__":
    delete_all_logs("../logs")
