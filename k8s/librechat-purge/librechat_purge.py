from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import argparse
import os
import re

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("LIBRECHAT_MONGO_URI", "mongodb://librechat-mongodb:27017")
DB_NAME = os.getenv("LIBRECHAT_DB_NAME", "LibreChat")
DAYS_STALE = int(os.getenv("STALE_DAYS", "180"))
IMAGES_PATH = os.getenv("IMAGES_PATH", "/images")
# Agent tool_resources keys that can reference file_ids (EToolResources)
TOOL_RESOURCE_KEYS = ("code_interpreter", "execute_code", "file_search",
                      "image_edit", "context", "ocr")
OBJECT_ID = re.compile(r"^[0-9a-f]{24}$", re.IGNORECASE)
# ──────────────────────────────────────────────────────────────────────────────


def get_stale_conversations(db, days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return list(db["conversations"].find(
        {"updatedAt": {"$lt": cutoff}},
        {"_id": 1, "conversationId": 1, "user": 1,
         "createdAt": 1, "updatedAt": 1},
    ).sort("updatedAt", 1))


def collect_files(db, conv_ids: list[str]) -> list[dict]:
    """Files attached to messages plus files generated in the conversations
    (tool/code outputs, image generation), which only carry a conversationId."""
    seen = set()
    results = []

    def add(file_id, filepath, source):
        if file_id and file_id not in seen:
            seen.add(file_id)
            results.append({
                "file_id": file_id,
                "filepath": filepath or "",
                "source": source or "local",
            })

    messages = db["messages"].find(
        {"conversationId": {"$in": conv_ids}},
        {"files": 1}
    )
    for message in messages:
        for file in message.get("files") or []:
            add(file.get("file_id"), file.get("filepath"), file.get("source"))

    generated = db["files"].find(
        {"conversationId": {"$in": conv_ids}},
        {"file_id": 1, "filepath": 1, "source": 1}
    )
    for file in generated:
        add(file.get("file_id"), file.get("filepath"), file.get("source"))

    # Message snapshots may lack filepath/source; fill them from the files collection
    missing = [f["file_id"] for f in results if not f["filepath"]]
    if missing:
        records = db["files"].find(
            {"file_id": {"$in": missing}},
            {"file_id": 1, "filepath": 1, "source": 1}
        )
        by_id = {r["file_id"]: r for r in records}
        for f in results:
            r = by_id.get(f["file_id"])
            if r and not f["filepath"]:
                f["filepath"] = r.get("filepath", "")
                f["source"] = r.get("source", f["source"])

    return results


def exclude_shared_files(db, files: list[dict], conv_ids: list[str]) -> list[dict]:
    """Drop files still referenced by a conversation that is not being purged,
    or attached to an agent's tool resources."""
    file_ids = [f["file_id"] for f in files]
    if not file_ids:
        return files

    in_use = set(db["messages"].distinct(
        "files.file_id",
        {"conversationId": {"$nin": conv_ids}, "files.file_id": {"$in": file_ids}},
    ))
    agent_filter = {"$or": [
        {f"tool_resources.{key}.file_ids": {"$in": file_ids}}
        for key in TOOL_RESOURCE_KEYS
    ]}
    for agent in db["agents"].find(agent_filter, {"tool_resources": 1}):
        for resource in (agent.get("tool_resources") or {}).values():
            in_use.update((resource or {}).get("file_ids") or [])

    kept = [f for f in files if f["file_id"] in in_use]
    if kept:
        print(f"  ℹ️  Keeping {len(kept)} file(s) still referenced elsewhere.")
    return [f for f in files if f["file_id"] not in in_use]


def resolve_disk_path(filepath: str, images_path: str) -> str | None:
    """Map a LibreChat local filepath to the mounted images volume.

    Images are stored as /images/<userId>/<file> under client/public/images,
    which is the only persisted volume. /uploads/... lives on the pod's
    ephemeral filesystem and has nothing to delete here.
    """
    clean = filepath.split("?", 1)[0]  # drop ?v=<timestamp> cache-bust suffix
    if not clean.startswith("/images/"):
        return None
    root = os.path.realpath(images_path)
    full = os.path.realpath(os.path.join(root, clean[len("/images/"):]))
    if os.path.commonpath([root, full]) != root:
        return None  # path traversal guard
    return full


def delete_files_directly(db, files: list[dict], images_path: str) -> tuple[int, int, int]:
    if not files:
        return 0, 0, 0

    file_ids = [f["file_id"] for f in files if f.get("file_id")]

    # Warn about non-local files that we can't delete from disk
    non_local = [f for f in files if f.get("source") not in ("local", "")]
    if non_local:
        print(f"  ⚠️  {len(non_local)} file(s) have non-local source and will only be removed from DB:")
        for f in non_local:
            print(f"      {f.get('source')} — {f.get('filepath')}")

    # Remove from MongoDB files collection
    db_result = db["files"].delete_many({"file_id": {"$in": file_ids}})

    # Remove from disk
    removed = 0
    missing = 0
    for f in files:
        if f.get("source") not in ("local", ""):
            continue
        full_path = resolve_disk_path(f.get("filepath", ""), images_path)
        if full_path is None:
            continue
        try:
            os.remove(full_path)
            removed += 1
        except FileNotFoundError:
            missing += 1
        except OSError as e:
            print(f"  ⚠️  Could not delete {full_path}: {e}")

    return db_result.deleted_count, removed, missing


def delete_conversations_from_db(db, conv_ids: list[str]) -> tuple[int, int]:
    messages = db["messages"].delete_many({"conversationId": {"$in": conv_ids}})
    conversations = db["conversations"].delete_many(
        {"conversationId": {"$in": conv_ids}}
    )
    return conversations.deleted_count, messages.deleted_count


def normalize_image_ref(ref) -> str | None:
    """Reduce a stored reference (possibly a full URL or with ?query) to /images/<userId>/<file>."""
    if not isinstance(ref, str) or "/images/" not in ref:
        return None
    return "/images/" + ref.split("/images/", 1)[1].split("?", 1)[0]


def collect_image_refs(db) -> set[str]:
    """Every /images/... path still referenced anywhere in the DB."""
    sources = [
        ("files", {"filepath": {"$regex": "/images/"}}, "filepath"),
        ("messages", {"files.filepath": {"$regex": "/images/"}}, "files.filepath"),
        ("users", {"avatar": {"$regex": "/images/"}}, "avatar"),
        ("agents", {"avatar.filepath": {"$regex": "/images/"}}, "avatar.filepath"),
        ("assistants", {"avatar.filepath": {"$regex": "/images/"}}, "avatar.filepath"),
    ]
    refs = set()
    for collection, query, field in sources:
        for doc in db[collection].find(query, {field: 1, "_id": 0}):
            values = [doc]
            for key in field.split("."):
                values = [
                    item
                    for v in values
                    for item in (v.get(key) if isinstance(v.get(key), list) else [v.get(key)])
                    if item is not None
                ]
            for value in values:
                ref = normalize_image_ref(value)
                if ref:
                    refs.add(ref)
    return refs


def find_orphan_images(db, images_path: str, min_age_days: int) -> list[str]:
    """Files under <images_path>/<userId>/ older than min_age_days that no DB record references."""
    refs = collect_image_refs(db)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=min_age_days)).timestamp()
    orphans = []
    with os.scandir(images_path) as user_dirs:
        for user_dir in user_dirs:
            if not user_dir.is_dir(follow_symlinks=False) or not OBJECT_ID.match(user_dir.name):
                continue
            with os.scandir(user_dir.path) as entries:
                for entry in entries:
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    if entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                        continue
                    if f"/images/{user_dir.name}/{entry.name}" not in refs:
                        orphans.append(entry.path)
    return sorted(orphans)


def sweep_orphan_images(db, args) -> None:
    print(f"\nOrphaned images under {args.images_path} older than {args.days} days")
    orphans = find_orphan_images(db, args.images_path, args.days)
    print(f"Found: {len(orphans)}")
    for path in orphans:
        print(f"  {path}")
    if not orphans or not args.delete:
        return

    if not args.yes:
        answer = input(f"⚠️  Permanently delete {len(orphans)} orphaned image(s)? [y/N] ")
        if answer.strip().lower() != "y":
            print("Aborted.")
            return

    removed = 0
    for path in orphans:
        try:
            os.remove(path)
            removed += 1
        except FileNotFoundError:
            pass
        except OSError as e:
            print(f"  ⚠️  Could not delete {path}: {e}")
    print(f"✅  Deleted {removed} orphaned image(s) from disk.")


def format_datetime(dt) -> str:
    if dt is None:
        return "n/a"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def print_table(convos: list[dict], show_ids: bool) -> None:
    hdr = f"{'Last updated':<20}  {'Created':<20}  {'User'}"
    if show_ids:
        hdr += "  ConversationId"
    print(hdr)
    print("-" * len(hdr))
    for c in convos:
        row = (f"{format_datetime(c.get('updatedAt')):<20}  "
               f"{format_datetime(c.get('createdAt')):<20}  "
               f"{c.get('user', '')}")
        if show_ids:
            row += f"  {c.get('conversationId', '')}"
        print(row)


def purge_conversations(db, args) -> None:
    # ── List ──────────────────────────────────────────────────────────────────
    convos = get_stale_conversations(db, args.days)
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)

    print(f"Conversations not updated since {format_datetime(cutoff)}  ({args.days} days ago)")
    print(f"Found: {len(convos)}\n")
    if not convos:
        return
    print_table(convos, args.show_ids)

    if not args.delete:
        return

    # ── Confirm ───────────────────────────────────────────────────────────────
    conv_ids = [c["conversationId"] for c in convos if c.get("conversationId")]
    files = exclude_shared_files(db, collect_files(db, conv_ids), conv_ids)

    print()
    if not args.yes:
        answer = input(
            f"⚠️  Permanently delete {len(convos)} conversation(s), "
            f"{len(files)} file(s) and their messages? [y/N] "
        ).strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    # ── Delete files ──────────────────────────────────────────────────────────
    print(f"Deleting {len(files)} file(s)...", end=" ", flush=True)
    del_file_records, del_file_disk, missing_on_disk = delete_files_directly(
        db, files, args.images_path)
    print("done.")
    if missing_on_disk:
        print(f"  ⚠️  {missing_on_disk} file(s) were not found under {args.images_path}.")

    # ── Delete DB records ─────────────────────────────────────────────────────
    print("Deleting DB records...", end=" ", flush=True)
    del_conversations, del_messages = delete_conversations_from_db(db, conv_ids)
    print("done.")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n✅  Deleted {del_conversations} conversation(s), {del_messages} message(s), "
          f"{del_file_records} file record(s) from DB, {del_file_disk} file(s) from disk.")


def main():
    parser = argparse.ArgumentParser(
        description="Delete stale LibreChat conversations and files directly via MongoDB + disk."
    )
    parser.add_argument("--uri", default=MONGO_URI, help="MongoDB URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    parser.add_argument("--days", default=DAYS_STALE, type=int,
                        help="Inactivity threshold in days (default: 180)")
    parser.add_argument("--images-path", default=IMAGES_PATH,
                        help=f"Mount path of LibreChat's images volume (default: {IMAGES_PATH})")
    parser.add_argument("--show-ids", action="store_true",
                        help="Print conversationId in table")
    parser.add_argument("--delete", action="store_true",
                        help="Perform deletion (default: list only)")
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt")
    parser.add_argument("--sweep-orphans", action="store_true",
                        help="Also remove images on disk that no DB record references "
                             "and that are older than --days")
    args = parser.parse_args()

    db = MongoClient(args.uri)[args.db]
    if args.delete or args.sweep_orphans:
        if not os.path.isdir(args.images_path):
            print(f"❌  Images path {args.images_path} is not mounted; refusing to delete.")
            raise SystemExit(1)

    purge_conversations(db, args)
    if args.sweep_orphans:
        sweep_orphan_images(db, args)


if __name__ == "__main__":
    main()