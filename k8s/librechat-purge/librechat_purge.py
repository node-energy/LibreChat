from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import argparse
import os
import time

import jwt
import requests

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("LIBRECHAT_MONGO_URI", "mongodb://librechat-mongodb:27017")
DB_NAME = os.getenv("LIBRECHAT_DB_NAME", "LibreChat")
DAYS_STALE = int(os.getenv("STALE_DAYS", "180"))
JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_USER_ID = os.getenv("LIBRECHAT_JWT_USER_ID", "")
LIBRECHAT_URL = os.getenv("LIBRECHAT_URL", "http://librechat-librechat:3080")
# ──────────────────────────────────────────────────────────────────────────────


def mint_token(secret: str, user_id: str) -> str:
    """Mint a JWT token using LibreChat's secret."""
    payload = {
        "id": user_id,       # LibreChat checks this field
        "userId": user_id,   # some middleware variants use this
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,  # 1 hour
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def get_stale_conversations(db, days: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return list(db["conversations"].find(
        {"updatedAt": {"$lt": cutoff}},
        {"_id": 1, "conversationId": 1, "title": 1, "user": 1,
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
                    "source": "local",
                    "embedded": file.get("embedded", False),
                })
    return results


def delete_files_via_api(files: list[dict], token: str, base_url: str) -> bool:
    if not files:
        return True
    resp = requests.delete(
        f"{base_url}/api/files",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Origin": base_url,
            "Referer": f"{base_url}/",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
        json={"files": files},
        timeout=30,
    )
    if not resp.ok:
        print(f"  ⚠️  API file deletion failed: {resp.status_code} {resp.text}")
        return False
    if "Illegal request" in resp.text:
        print(f"  ⚠️  API rejected request: {resp.text}")
        return False
    return True


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
    w = max((len(c.get("title") or "Untitled") for c in convos), default=30)
    w = max(w, 30)
    hdr = f"{'Last updated':<20}  {'Created':<20}  {'Title':<{w}}  {'User'}"
    if show_ids:
        hdr += "  ConversationId"
    print(hdr)
    print("-" * len(hdr))
    for c in convos:
        row = (f"{format_datetime(c.get('updatedAt')):<20}  "
               f"{format_datetime(c.get('createdAt')):<20}  "
               f"{(c.get('title') or 'Untitled')[:w]:<{w}}  "
               f"{c.get('user', '')}")
        if show_ids:
            row += f"  {c.get('conversationId', '')}"
        print(row)


def main():
    parser = argparse.ArgumentParser(
        description="Delete stale LibreChat conversations via API + MongoDB."
    )
    parser.add_argument("--uri", default=MONGO_URI, help="MongoDB URI")
    parser.add_argument("--db", default=DB_NAME, help="Database name")
    parser.add_argument("--days", default=DAYS_STALE, type=int,
                        help="Inactivity threshold in days (default: 180)")
    parser.add_argument("--url", default=LIBRECHAT_URL,
                        help=f"LibreChat base URL (default: {LIBRECHAT_URL})")
    parser.add_argument("--jwt-secret", default=JWT_SECRET,
                        help="LibreChat JWT secret (or set LIBRECHAT_JWT_SECRET)")
    parser.add_argument("--user-id", default=JWT_USER_ID,
                        help="User ObjectId to embed in token (or set LIBRECHAT_JWT_USER_ID)")
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

    # ── Validate auth args ────────────────────────────────────────────────────
    if not args.jwt_secret:
        print("\n❌  --jwt-secret (or LIBRECHAT_JWT_SECRET) is required for deletion.")
        return
    if not args.user_id:
        print("\n❌  --user-id (or LIBRECHAT_JWT_USER_ID) is required for deletion.")
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

    # ── Delete files via API ──────────────────────────────────────────────────
    token = mint_token(args.jwt_secret, args.user_id)
    print(f"Deleting {len(files)} file(s) via API...", end=" ", flush=True)
    files_ok = delete_files_via_api(files, token, args.url)
    print("done." if files_ok else "failed (see above).")

    # ── Delete DB records ─────────────────────────────────────────────────────
    print("Deleting DB records...", end=" ", flush=True)
    del_conversations, del_messages = delete_conversations_from_db(db, conv_ids)
    print("done.")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    if files_ok:
        print(f"✅  Deleted {del_conversations} conversation(s), "
              f"{del_messages} message(s), {len(files)} file(s) via API.")
    else:
        print(f"⚠️  Partially completed: deleted {del_conversations} conversation(s) "
              f"and {del_messages} message(s) from DB, but file deletion via API failed — "
              f"{len(files)} file(s) may still exist on disk.")


if __name__ == "__main__":
    main()