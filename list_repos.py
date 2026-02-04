import os
import requests
from dotenv import load_dotenv

load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_PAT = os.getenv("GITLAB_PAT")

headers = {"PRIVATE-TOKEN": GITLAB_PAT}

page = 1
per_page = 100
total = 0

while True:
    response = requests.get(
        f"{GITLAB_URL}/api/v4/projects",
        headers=headers,
        params={"page": page, "per_page": per_page, "membership": True},
    )
    response.raise_for_status()

    projects = response.json()
    if not projects:
        break

    for p in projects:
        total += 1
        print(f"{total}. {p['path_with_namespace']} (id={p['id']})")

    page += 1

print(f"\nTotal: {total} repositórios")
