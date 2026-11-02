"""Purge stale LibreChat conversations and their files directly via MongoDB + disk.

Steps (each lists first and only deletes with --delete):
  1. conversations not updated for --days, with their messages and files
  2. --purge-unused-files: chat files whose conversation is gone (e.g. deleted by the user)
  3. --sweep-orphans: files on disk that no DB record references

A file's DB record is only removed once the file is verifiably gone from disk,
so failed deletes are retried on the next run and make the job exit 1.
"""
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import argparse
import os
import re

MONGO_URI = os.getenv("LIBRECHAT_MONGO_URI", "mongodb://librechat-mongodb:27017")
DB_NAME = os.getenv("LIBRECHAT_DB_NAME", "LibreChat")
DAYS_STALE = int(os.getenv("STALE_DAYS", "180"))
IMAGES_PATH = os.getenv("IMAGES_PATH", "/images")
UPLOADS_PATH = os.getenv("UPLOADS_PATH", "")

OBJECT_ID = re.compile(r"^[0-9a-f]{24}$", re.IGNORECASE)
# File contexts created by chatting (FileContext); agent, assistant, skill and avatar files are never touched
CHAT_FILE_CONTEXTS = ["message_attachment", "image_generation", "execute_code", "run_artifact"]
# Agent tool_resources keys that can reference file_ids (EToolResources)
TOOL_RESOURCE_KEYS = ["code_interpreter", "execute_code", "file_search", "image_edit", "context", "ocr"]
# (collection, field, scoped to conversations) holding file_ids
FILE_ID_REFS = [
    ("messages", "files.file_id", True),
    ("messages", "attachments.file_id", True),
    ("conversations", "files", True),
    ("conversations", "file_ids", True),
    ("presets", "file_ids", False),
    ("chatprojects", "file_ids", False),
    ("schedules", "file_ids", False),
    ("assistants", "file_ids", False),
] + [("agents", f"tool_resources.{key}.file_ids", False) for key in TOOL_RESOURCE_KEYS]
# (collection, field) holding /images/... or /uploads/... paths
FILEPATH_REFS = [
    ("files", "filepath"),
    ("messages", "files.filepath"),
    ("messages", "attachments.filepath"),
    ("users", "avatar"),
    ("agents", "avatar.filepath"),
    ("assistants", "avatar.filepath"),
]
FILE_FIELDS = {"_id": 0, "file_id": 1, "filepath": 1, "source": 1,
               "conversationId": 1, "user": 1, "updatedAt": 1}


# ── Helpers ───────────────────────────────────────────────────────────────────

def cutoff(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


def fmt(dt) -> str:
    if dt is None:
        return "n/a"
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).strftime("%Y-%m-%d %H:%M UTC")


def confirm(args, question: str) -> bool:
    if args.yes:
        return True
    if input(f"⚠️  {question} [y/N] ").strip().lower() == "y":
        return True
    print("Aborted.")
    return False


def values_at(doc: dict, path: str) -> list:
    """All values at a dotted path, flattening arrays along the way."""
    values = [doc]
    for key in path.split("."):
        nxt = []
        for v in values:
            item = v.get(key) if isinstance(v, dict) else None
            nxt.extend(item if isinstance(item, list) else [item])
        values = [v for v in nxt if v is not None]
    return values


# ── References ────────────────────────────────────────────────────────────────

def referenced_file_ids(db, file_ids: list[str], ignore_conv_ids: list[str]) -> set[str]:
    """file_ids still used anywhere, except by the conversations in ignore_conv_ids."""
    in_use = set()
    for collection, field, scoped in FILE_ID_REFS:
        query = {field: {"$in": file_ids}}
        if scoped:
            query["conversationId"] = {"$nin": ignore_conv_ids}
        for doc in db[collection].find(query, {field: 1, "_id": 0}):
            in_use.update(values_at(doc, field))
    return in_use & set(file_ids)


def referenced_paths(db, prefixes: list[str]) -> set[str]:
    """Every referenced <userId>/<file> under any prefix, normalized (no host, no ?query).

    The prefix is dropped on purpose: a file counts as referenced no matter which
    volume it is found on, so a misconfigured mount can never make a referenced
    file look orphaned.
    """
    pattern = "|".join(re.escape(p) for p in prefixes)
    refs = set()
    for collection, field in FILEPATH_REFS:
        for doc in db[collection].find({field: {"$regex": pattern}}, {field: 1, "_id": 0}):
            for value in values_at(doc, field):
                prefix = next((p for p in prefixes if isinstance(value, str) and p in value), None)
                if prefix:
                    refs.add(value.split(prefix, 1)[1].split("?", 1)[0])
    return refs


# ── Disk ──────────────────────────────────────────────────────────────────────

def disk_path(filepath: str, roots: dict) -> tuple[str | None, str | None]:
    """(full_path, prefix) for a local filepath; full_path is None if the prefix is
    unknown, its volume is not mounted, or the path escapes its root."""
    clean = filepath.split("?", 1)[0]  # drop ?v=<timestamp> cache-bust suffix
    prefix = next((p for p in roots if clean.startswith(p)), None)
    if not prefix or not roots[prefix]:
        return None, prefix
    root = os.path.realpath(roots[prefix])
    full = os.path.realpath(os.path.join(root, clean[len(prefix):]))
    return (full if os.path.commonpath([root, full]) == root else None), prefix


def remove(path: str) -> str:
    """Delete a file and verify it is gone: 'removed', 'missing' or 'failed'."""
    try:
        os.remove(path)
    except FileNotFoundError:
        return "missing"
    except OSError as e:
        print(f"  ❌  Could not delete {path}: {e}")
        return "failed"
    if os.path.lexists(path):
        print(f"  ❌  {path} still exists after delete")
        return "failed"
    return "removed"


def delete_files(db, files: list[dict], roots: dict, label: str) -> int:
    """Delete files from disk, then the DB records of those that are gone. Returns failures."""
    stats = {"removed": 0, "missing": 0, "failed": 0}
    deletable = []
    for f in files:
        filepath = f.get("filepath") or ""
        path, prefix = disk_path(filepath, roots)
        if (f.get("source") or "local") != "local":
            print(f"  ⚠️  Non-local file, removing DB record only: {f.get('source')} — {filepath}")
        elif prefix is None:
            print(f"  ⚠️  Unknown local path, removing DB record only: {filepath!r}")
        elif path is None:
            reason = "path escapes its volume" if roots[prefix] else f"volume for {prefix} is not mounted"
            print(f"  ❌  {filepath}: {reason}, keeping DB record")
            stats["failed"] += 1
            continue
        else:
            result = remove(path)
            stats[result] += 1
            if result == "failed":
                continue
        deletable.append(f["file_id"])

    records = db["files"].delete_many({"file_id": {"$in": deletable}}).deleted_count if deletable else 0
    print(f"✅  Deleted {records} {label} record(s) from DB, {stats['removed']} file(s) from disk.")
    if stats["missing"]:
        print(f"  ℹ️  {stats['missing']} file(s) were already gone from disk.")
    if stats["failed"]:
        print(f"  ❌  {stats['failed']} file(s) could not be removed from disk; "
              f"their DB records were kept for the next run.")
    return stats["failed"]


# ── Steps ─────────────────────────────────────────────────────────────────────

def conversation_files(db, conv_ids: list[str]) -> list[dict]:
    """Files attached to or generated in the conversations, minus files still used elsewhere."""
    snapshots = {}
    for m in db["messages"].find({"conversationId": {"$in": conv_ids}}, {"files": 1}):
        for f in m.get("files") or []:
            if f.get("file_id"):
                snapshots.setdefault(f["file_id"], f)
    # The files collection is authoritative; message snapshots cover files without a record
    records = db["files"].find(
        {"$or": [{"file_id": {"$in": list(snapshots)}}, {"conversationId": {"$in": conv_ids}}]},
        FILE_FIELDS,
    )
    files = {**snapshots, **{f["file_id"]: f for f in records if f.get("file_id")}}

    in_use = referenced_file_ids(db, list(files), conv_ids)
    if in_use:
        print(f"  ℹ️  Keeping {len(in_use)} file(s) still referenced elsewhere.")
    return [f for fid, f in files.items() if fid not in in_use]


def purge_conversations(db, args, roots: dict) -> int:
    convos = list(db["conversations"].find(
        {"updatedAt": {"$lt": cutoff(args.days)}},
        {"_id": 0, "conversationId": 1, "user": 1, "createdAt": 1, "updatedAt": 1},
    ).sort("updatedAt", 1))
    print(f"Conversations not updated since {fmt(cutoff(args.days))}  ({args.days} days ago)")
    print(f"Found: {len(convos)}\n")
    if not convos:
        return 0

    ids_col = "  ConversationId" if args.show_ids else ""
    hdr = f"{'Last updated':<20}  {'Created':<20}  User{ids_col}"
    print(f"{hdr}\n{'-' * len(hdr)}")
    for c in convos:
        conv_id = f"  {c.get('conversationId', '')}" if args.show_ids else ""
        print(f"{fmt(c.get('updatedAt')):<20}  {fmt(c.get('createdAt')):<20}  {c.get('user', '')}{conv_id}")
    if not args.delete:
        return 0

    conv_ids = [c["conversationId"] for c in convos if c.get("conversationId")]
    files = conversation_files(db, conv_ids)
    print()
    if not confirm(args, f"Permanently delete {len(convos)} conversation(s), "
                         f"{len(files)} file(s) and their messages?"):
        return 0

    # Records of files that fail to delete are kept; --purge-unused-files retries them
    print(f"Deleting {len(files)} file(s)...")
    failed = delete_files(db, files, roots, "file")
    messages = db["messages"].delete_many({"conversationId": {"$in": conv_ids}}).deleted_count
    conversations = db["conversations"].delete_many({"conversationId": {"$in": conv_ids}}).deleted_count
    print(f"✅  Deleted {conversations} conversation(s), {messages} message(s).")
    return failed


def purge_unused_files(db, args, roots: dict) -> int:
    """Chat files not updated for --days whose conversation is gone (deleting a chat
    in the UI keeps its file records) and that nothing else references."""
    print(f"\nChat files unused since {args.days} days whose conversation no longer exists")
    candidates = list(db["files"].find(
        {"context": {"$in": CHAT_FILE_CONTEXTS}, "updatedAt": {"$lt": cutoff(args.days)},
         "file_id": {"$nin": [None, ""]}},
        FILE_FIELDS,
    ).sort("updatedAt", 1))
    conv_ids = list({c["conversationId"] for c in candidates if c.get("conversationId")})
    live = set(db["conversations"].distinct("conversationId", {"conversationId": {"$in": conv_ids}}))
    in_use = referenced_file_ids(db, [c["file_id"] for c in candidates], [])
    files = [c for c in candidates if c.get("conversationId") not in live and c["file_id"] not in in_use]

    print(f"Found: {len(files)}")
    for f in files:
        print(f"  {fmt(f.get('updatedAt')):<20}  {f.get('user', '')}  {f.get('filepath', '')}")
    if not files or not args.delete or not confirm(args, f"Permanently delete {len(files)} unused file(s)?"):
        return 0
    return delete_files(db, files, roots, "unused file")


def sweep_orphans(db, args, roots: dict) -> int:
    """Files under <mount>/<userId>/ older than --days that no DB record references."""
    mounted = {prefix: mount for prefix, mount in roots.items() if mount}
    print(f"\nOrphaned files under {', '.join(mounted.values())} older than {args.days} days")
    refs = referenced_paths(db, list(roots))
    limit = cutoff(args.days).timestamp()
    orphans = []
    for mount in mounted.values():
        for user in os.scandir(mount):
            if not user.is_dir(follow_symlinks=False) or not OBJECT_ID.match(user.name):
                continue
            for entry in os.scandir(user.path):
                if (entry.is_file(follow_symlinks=False)
                        and entry.stat(follow_symlinks=False).st_mtime < limit
                        and f"{user.name}/{entry.name}" not in refs):
                    orphans.append(entry.path)
    orphans.sort()

    print(f"Found: {len(orphans)}")
    for path in orphans:
        print(f"  {path}")
    if not orphans or not args.delete or not confirm(args, f"Permanently delete {len(orphans)} orphaned file(s)?"):
        return 0
    results = [remove(path) for path in orphans]
    failed = results.count("failed")
    print(f"✅  Deleted {results.count('removed')} orphaned file(s) from disk.")
    if failed:
        print(f"  ❌  {failed} orphaned file(s) could not be removed.")
    return failed


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--uri", default=MONGO_URI, help="MongoDB URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    parser.add_argument("--days", default=DAYS_STALE, type=int,
                        help="Inactivity threshold in days (default: 180)")
    parser.add_argument("--images-path", default=IMAGES_PATH,
                        help=f"Mount path of LibreChat's images volume (default: {IMAGES_PATH})")
    parser.add_argument("--uploads-path", default=UPLOADS_PATH,
                        help="Mount path of LibreChat's uploads volume (PDFs, documents); "
                             "unset = not mounted, those files keep their DB record")
    parser.add_argument("--show-ids", action="store_true", help="Print conversationId in table")
    parser.add_argument("--delete", action="store_true", help="Perform deletion (default: list only)")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")
    parser.add_argument("--purge-unused-files", action="store_true",
                        help="Also remove chat files unused for --days whose conversation "
                             "no longer exists (agent/assistant/skill/avatar files are kept)")
    parser.add_argument("--sweep-orphans", action="store_true",
                        help="Also remove files on disk that no DB record references "
                             "and that are older than --days")
    args = parser.parse_args()

    # LibreChat filepath prefix -> mount path in this container (None = not mounted)
    roots = {"/images/": args.images_path, "/uploads/": args.uploads_path or None}
    if args.delete or args.sweep_orphans:
        for prefix, path in roots.items():
            if path and not os.path.isdir(path):
                print(f"❌  {prefix.strip('/').capitalize()} path {path} is not mounted; refusing to delete.")
                raise SystemExit(1)
        mounted = [os.path.realpath(p) for p in roots.values() if p]
        for i, a in enumerate(mounted):
            for b in mounted[i + 1:]:
                if os.path.samefile(a, b) or os.path.commonpath([a, b]) in (a, b):
                    print(f"❌  {a} and {b} are the same or nested volumes; images and uploads "
                          f"must be separate directories. Refusing to delete.")
                    raise SystemExit(1)
        if not args.uploads_path:
            print("⚠️  UPLOADS_PATH not set: files under /uploads (PDFs, documents) "
                  "are not deleted from disk and keep their DB record.\n")

    db = MongoClient(args.uri)[args.db]
    failed = purge_conversations(db, args, roots)
    if args.purge_unused_files:
        failed += purge_unused_files(db, args, roots)
    if args.sweep_orphans:
        failed += sweep_orphans(db, args, roots)

    if failed:
        print(f"\n❌  {failed} file(s) could not be removed from disk, see above.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
