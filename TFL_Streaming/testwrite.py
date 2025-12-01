import os
from datetime import datetime

def main():
    path = "uk/streaming/test"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/placeholder_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(filename, "w") as f:
        f.write("This is a placeholder file.\n")
    print(f"Created {filename}")

if __name__ == "__main__":
    main()
