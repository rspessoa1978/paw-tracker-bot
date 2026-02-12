import os
from pathlib import Path

scopus_dir = Path.home() / ".scopus"
scopus_dir.mkdir(exist_ok=True)
config_file = scopus_dir / "config.ini"

with open(config_file, "w") as f:
    f.write(f"[Authentication]\nAPIKey = {os.environ['SCOPUS_API_KEY']}\n")