from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import argparse
import os

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("LIBRECHAT_MONGO_URI", "mongodb://librechat-mongodb:27017")
DB_NAME = os.getenv("LIBRECHAT_DB_NAME", "LibreChat")
DAYS_STALE = int(os.getenv("STALE_DAYS", "180"))
UPLOADS_PATH = os.getenv("UPLOADS_PATH", "/uploads")
# ──────────────────────────────────────────────────────────────────────────────


def get_stale_conversations(db, days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return list(db["conversations"].find(
        {"updatedAt": {"$lt": cutoff}},
        {"_id": 1, "conversationId": 1, "user": 1,
         "createdAt": 1, "updatedAt": 1},
    ).sort("updatedAt", 1))


def collect_files(db, conv_ids: list[str]) -> list[dict]:
    messages = db["messages"].find(
        {"conversationId": {"$in": conv_ids}},
        {"files": 1}
    )
    seen = set()
    results = []
    for message in messages:
        for file in message.get("files") or []:
            file_id = file.get("file_id")
            if file_id and file_id not in seen:
                seen.add(file_id)
                results.append({
                    "file_id": file_id,
                    "filepath": file.get("filepath", ""),
                    "source": file.get("source", "local"),
                    "embedded": file.get("embedded", False),
                })
    return results


def delete_files_directly(db, files: list[dict], uploads_path: str) -> tuple[int, int]:
    if not files:
        return 0, 0

    file_ids = [f["file_id"] for f in files if f.get("file_id")]
    filepaths = [f["filepath"] for f in files if f.get("filepath")]

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
    for filepath in filepaths:
        full_path = os.path.join(uploads_path, filepath.lstrip("/"))
        try:
            os.remove(full_path)
            removed += 1
        except FileNotFoundError:
            pass  # already gone
        except OSError as e:
            print(f"  ⚠️  Could not delete {full_path}: {e}")

    return db_result.deleted_count, removed


def delete_conversations_from_db(db, conv_ids: list[str]) -> tuple[int, int]:
    messages = db["messages"].delete_many({"conversationId": {"$in": conv_ids}})
    conversations = db["conversations"].delete_many(
        {"conversationId": {"$in": conv_ids}}
    )
    return conversations.deleted_count, messages.deleted_count


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


def main():
    parser = argparse.ArgumentParser(
        description="Delete stale LibreChat conversations and files directly via MongoDB + disk."
    )
    parser.add_argument("--uri", default=MONGO_URI, help="MongoDB URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    parser.add_argument("--days", default=DAYS_STALE, type=int,
                        help="Inactivity threshold in days (default: 180)")
    parser.add_argument("--uploads-path", default=UPLOADS_PATH,
                        help=f"Path to LibreChat uploads directory (default: {UPLOADS_PATH})")
    parser.add_argument("--show-ids", action="store_true",
                        help="Print conversationId in table")
    parser.add_argument("--delete", action="store_true",
                        help="Perform deletion (default: list only)")
    parser.add_argument("--yes", action="store_true",
                        help="Skip confirmation prompt")
    args = parser.parse_args()

    # ── List ──────────────────────────────────────────────────────────────────
    db = MongoClient(args.uri)[args.db]
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
    files = collect_files(db, conv_ids)

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
    del_file_records, del_file_disk = delete_files_directly(db, files, args.uploads_path)
    print("done.")

    # ── Delete DB records ─────────────────────────────────────────────────────
    print("Deleting DB records...", end=" ", flush=True)
    del_conversations, del_messages = delete_conversations_from_db(db, conv_ids)
    print("done.")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n✅  Deleted {del_conversations} conversation(s), {del_messages} message(s), "
          f"{del_file_records} file record(s) from DB, {del_file_disk} file(s) from disk.")


if __name__ == "__main__":
    main()