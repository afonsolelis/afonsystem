import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_PAT = os.getenv("GITLAB_PAT")
MONGODB_URI = os.getenv("MONGODB_URI")

headers = {"PRIVATE-TOKEN": GITLAB_PAT}
client = MongoClient(MONGODB_URI)
db = client["afonsystem"]


def paginated_get(url, params=None):
    """Busca paginada na API do GitLab. Retorna todos os resultados."""
    if params is None:
        params = {}
    params["per_page"] = 100
    params["page"] = 1
    results = []
    while True:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        results.extend(data)
        params["page"] += 1
    return results


def fetch_projects():
    """Busca todos os projetos acessíveis."""
    return paginated_get(f"{GITLAB_URL}/api/v4/projects", {"membership": True})


def extract_project(project):
    """Salva metadados do projeto."""
    doc = {
        "project_id": project["id"],
        "name": project["name"],
        "path_with_namespace": project["path_with_namespace"],
        "description": project.get("description"),
        "created_at": project["created_at"],
        "default_branch": project.get("default_branch"),
        "web_url": project["web_url"],
    }
    db.projects.update_one(
        {"project_id": doc["project_id"]}, {"$set": doc}, upsert=True
    )
    return doc


def extract_members(project_id):
    """Busca e salva membros do projeto."""
    members = paginated_get(f"{GITLAB_URL}/api/v4/projects/{project_id}/members/all")
    if not members:
        return 0
    ops = []
    for m in members:
        doc = {
            "project_id": project_id,
            "user_id": m["id"],
            "username": m["username"],
            "name": m["name"],
            "access_level": m["access_level"],
        }
        ops.append(
            UpdateOne(
                {"project_id": project_id, "user_id": m["id"]},
                {"$set": doc},
                upsert=True,
            )
        )
    db.members.bulk_write(ops)
    return len(ops)


def extract_commits(project_id):
    """Busca e salva commits de todas as branches."""
    commits = paginated_get(
        f"{GITLAB_URL}/api/v4/projects/{project_id}/repository/commits",
        {"all": True, "with_stats": True},
    )
    if not commits:
        return 0
    ops = []
    for c in commits:
        stats = c.get("stats") or {}
        doc = {
            "project_id": project_id,
            "sha": c["id"],
            "short_id": c["short_id"],
            "author_name": c["author_name"],
            "author_email": c["author_email"],
            "committed_date": c["committed_date"],
            "message": c["message"],
            "additions": stats.get("additions", 0),
            "deletions": stats.get("deletions", 0),
            "total": stats.get("total", 0),
        }
        ops.append(
            UpdateOne(
                {"project_id": project_id, "sha": c["id"]},
                {"$set": doc},
                upsert=True,
            )
        )
    db.commits.bulk_write(ops)
    return len(ops)


def extract_merge_requests(project_id):
    """Busca e salva merge requests."""
    mrs = paginated_get(
        f"{GITLAB_URL}/api/v4/projects/{project_id}/merge_requests",
        {"state": "all"},
    )
    if not mrs:
        return 0
    ops = []
    for mr in mrs:
        author = mr.get("author") or {}
        doc = {
            "project_id": project_id,
            "iid": mr["iid"],
            "title": mr["title"],
            "author_username": author.get("username"),
            "author_name": author.get("name"),
            "state": mr["state"],
            "created_at": mr["created_at"],
            "merged_at": mr.get("merged_at"),
            "closed_at": mr.get("closed_at"),
            "source_branch": mr["source_branch"],
            "target_branch": mr["target_branch"],
            "merge_commit_sha": mr.get("merge_commit_sha"),
        }
        ops.append(
            UpdateOne(
                {"project_id": project_id, "iid": mr["iid"]},
                {"$set": doc},
                upsert=True,
            )
        )
    db.merge_requests.bulk_write(ops)
    return len(ops)


def extract_issues(project_id):
    """Busca e salva issues."""
    issues = paginated_get(
        f"{GITLAB_URL}/api/v4/projects/{project_id}/issues",
        {"state": "all"},
    )
    if not issues:
        return 0
    ops = []
    for issue in issues:
        author = issue.get("author") or {}
        assignees = [
            {"username": a["username"], "name": a["name"]}
            for a in (issue.get("assignees") or [])
        ]
        doc = {
            "project_id": project_id,
            "iid": issue["iid"],
            "title": issue["title"],
            "author_username": author.get("username"),
            "author_name": author.get("name"),
            "assignees": assignees,
            "state": issue["state"],
            "labels": issue.get("labels", []),
            "created_at": issue["created_at"],
            "closed_at": issue.get("closed_at"),
        }
        ops.append(
            UpdateOne(
                {"project_id": project_id, "iid": issue["iid"]},
                {"$set": doc},
                upsert=True,
            )
        )
    db.issues.bulk_write(ops)
    return len(ops)


def main():
    projects = fetch_projects()
    print(f"Encontrados {len(projects)} projetos\n")

    for project in projects:
        pid = project["id"]
        name = project["path_with_namespace"]
        print(f"--- {name} (id={pid}) ---")

        extract_project(project)
        print("  projeto salvo")

        n = extract_members(pid)
        print(f"  {n} membros")

        n = extract_commits(pid)
        print(f"  {n} commits")

        n = extract_merge_requests(pid)
        print(f"  {n} merge requests")

        n = extract_issues(pid)
        print(f"  {n} issues")

        print()

    # Resumo final
    print("=== Resumo no MongoDB (database: afonsystem) ===")
    for col_name in ["projects", "members", "commits", "merge_requests", "issues"]:
        count = db[col_name].count_documents({})
        print(f"  {col_name}: {count} documentos")

    client.close()


if __name__ == "__main__":
    main()
