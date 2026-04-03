import csv
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def format_datetime(value: str | None) -> str:
    if not value:
        return ""

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value

    return parsed.strftime("%d.%m.%Y %H:%M:%S")


def period_start_iso(days: int) -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start_of_today = now.replace(hour=0, minute=0, second=0)
    if days <= 1:
        return start_of_today.isoformat()
    return (start_of_today - timedelta(days=days - 1)).isoformat()


class Database:
    def __init__(self, path: str | Path = "bot_data.sqlite3"):
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def init(self) -> None:
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    start_count INTEGER NOT NULL DEFAULT 0,
                    subscription_verified_at TEXT,
                    lead_magnet_sent_at TEXT,
                    lead_magnet_sent INTEGER NOT NULL DEFAULT 0,
                    last_delivery_status TEXT,
                    last_delivery_error TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    details TEXT,
                    FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_events_type_created
                ON events (event_type, created_at)
                """
            )
            connection.commit()

    def upsert_user(self, telegram_id: int, username: str | None, first_name: str | None) -> None:
        now = utc_now_iso()
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO users (
                    telegram_id, username, first_name, first_seen_at, last_seen_at, start_count
                ) VALUES (?, ?, ?, ?, ?, 0)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_seen_at = excluded.last_seen_at
                """,
                (telegram_id, username, first_name, now, now),
            )
            connection.commit()

    def record_start(self, telegram_id: int, username: str | None, first_name: str | None) -> None:
        now = utc_now_iso()
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO users (
                    telegram_id, username, first_name, first_seen_at, last_seen_at, start_count
                ) VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_seen_at = excluded.last_seen_at,
                    start_count = users.start_count + 1
                """,
                (telegram_id, username, first_name, now, now),
            )
            cursor.execute(
                """
                INSERT INTO events (telegram_id, event_type, created_at)
                VALUES (?, 'start_clicked', ?)
                """,
                (telegram_id, now),
            )
            connection.commit()

    def record_subscription_verified(self, telegram_id: int) -> None:
        now = utc_now_iso()
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                UPDATE users
                SET subscription_verified_at = COALESCE(subscription_verified_at, ?),
                    last_seen_at = ?
                WHERE telegram_id = ?
                """,
                (now, now, telegram_id),
            )
            cursor.execute(
                """
                INSERT INTO events (telegram_id, event_type, created_at)
                VALUES (?, 'subscription_verified', ?)
                """,
                (telegram_id, now),
            )
            connection.commit()

    def record_lead_magnet_sent(self, telegram_id: int) -> None:
        now = utc_now_iso()
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                UPDATE users
                SET lead_magnet_sent_at = COALESCE(lead_magnet_sent_at, ?),
                    lead_magnet_sent = 1,
                    last_seen_at = ?,
                    last_delivery_status = 'sent',
                    last_delivery_error = NULL
                WHERE telegram_id = ?
                """,
                (now, now, telegram_id),
            )
            cursor.execute(
                """
                INSERT INTO events (telegram_id, event_type, created_at)
                VALUES (?, 'lead_magnet_sent', ?)
                """,
                (telegram_id, now),
            )
            connection.commit()

    def record_delivery_failure(self, telegram_id: int, error_text: str) -> None:
        now = utc_now_iso()
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                UPDATE users
                SET last_seen_at = ?,
                    last_delivery_status = 'failed',
                    last_delivery_error = ?
                WHERE telegram_id = ?
                """,
                (now, error_text[:500], telegram_id),
            )
            cursor.execute(
                """
                INSERT INTO events (telegram_id, event_type, created_at, details)
                VALUES (?, 'lead_magnet_failed', ?, ?)
                """,
                (telegram_id, now, error_text[:500]),
            )
            connection.commit()

    def get_stats(self) -> dict[str, int]:
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM events WHERE event_type = 'start_clicked'")
            start_total = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users")
            unique_users = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users WHERE subscription_verified_at IS NOT NULL")
            subscription_verified = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users WHERE lead_magnet_sent = 1")
            lead_magnet_sent = cursor.fetchone()[0]

            return {
                "start_total": start_total,
                "unique_users": unique_users,
                "subscription_verified": subscription_verified,
                "lead_magnet_sent": lead_magnet_sent,
            }

    def get_period_stats(self, start_at: str | None = None) -> dict[str, int]:
        with closing(self.connect()) as connection:
            cursor = connection.cursor()

            if start_at is None:
                cursor.execute("SELECT COUNT(*) FROM events WHERE event_type = 'start_clicked'")
                start_total = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM users")
                unique_users = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM users WHERE subscription_verified_at IS NOT NULL")
                subscription_verified = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM users WHERE lead_magnet_sent = 1")
                lead_magnet_sent = cursor.fetchone()[0]
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM events
                    WHERE event_type = 'start_clicked' AND created_at >= ?
                    """,
                    (start_at,),
                )
                start_total = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM users
                    WHERE first_seen_at >= ?
                    """,
                    (start_at,),
                )
                unique_users = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM users
                    WHERE subscription_verified_at IS NOT NULL
                      AND subscription_verified_at >= ?
                    """,
                    (start_at,),
                )
                subscription_verified = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM users
                    WHERE lead_magnet_sent = 1
                      AND lead_magnet_sent_at IS NOT NULL
                      AND lead_magnet_sent_at >= ?
                    """,
                    (start_at,),
                )
                lead_magnet_sent = cursor.fetchone()[0]

            return {
                "start_total": start_total,
                "unique_users": unique_users,
                "subscription_verified": subscription_verified,
                "lead_magnet_sent": lead_magnet_sent,
            }

    def export_users_csv(self, destination: str | Path) -> Path:
        destination = Path(destination)
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT
                    telegram_id,
                    username,
                    first_name,
                    first_seen_at,
                    last_seen_at,
                    start_count,
                    subscription_verified_at,
                    lead_magnet_sent_at,
                    lead_magnet_sent,
                    last_delivery_status,
                    last_delivery_error
                FROM users
                ORDER BY first_seen_at ASC
                """
            )
            rows = cursor.fetchall()

        with destination.open("w", newline="", encoding="utf-8-sig") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "Telegram ID",
                    "Username",
                    "Имя",
                    "Дата первого входа",
                    "Дата последней активности",
                    "Количество /start",
                    "Дата подтверждения подписки",
                    "Дата получения лидмагнита",
                    "Лидмагнит отправлен",
                    "Статус последней отправки",
                    "Текст последней ошибки",
                ]
            )
            for row in rows:
                writer.writerow(
                    [
                        row["telegram_id"],
                        row["username"],
                        row["first_name"],
                        format_datetime(row["first_seen_at"]),
                        format_datetime(row["last_seen_at"]),
                        row["start_count"],
                        format_datetime(row["subscription_verified_at"]),
                        format_datetime(row["lead_magnet_sent_at"]),
                        row["lead_magnet_sent"],
                        row["last_delivery_status"],
                        row["last_delivery_error"],
                    ]
                )

        return destination

    def get_broadcast_recipients(self) -> list[int]:
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT telegram_id
                FROM users
                WHERE last_delivery_status IS NULL
                   OR last_delivery_status != 'blocked'
                ORDER BY first_seen_at ASC
                """
            )
            rows = cursor.fetchall()

        return [row["telegram_id"] for row in rows]

    def record_broadcast_result(self, telegram_id: int, status: str, error_text: str | None = None) -> None:
        now = utc_now_iso()
        with closing(self.connect()) as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                UPDATE users
                SET last_seen_at = ?,
                    last_delivery_status = ?,
                    last_delivery_error = ?
                WHERE telegram_id = ?
                """,
                (now, status, error_text[:500] if error_text else None, telegram_id),
            )
            cursor.execute(
                """
                INSERT INTO events (telegram_id, event_type, created_at, details)
                VALUES (?, 'broadcast_delivery', ?, ?)
                """,
                (telegram_id, now, status if error_text is None else f"{status}: {error_text[:500]}"),
            )
            connection.commit()
