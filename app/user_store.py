from __future__ import annotations

import base64
import binascii
import uuid
from datetime import datetime

import pymysql

from app.config import settings


class UserStoreError(Exception):
    pass


class UserNotFoundError(UserStoreError):
    pass


class UserConflictError(UserStoreError):
    pass


class UserValidationError(UserStoreError):
    pass


class MySQLUserStore:
    def _connect(self):
        return pymysql.connect(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            database=settings.mysql_db,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            init_command="SET time_zone = '+08:00'",
        )

    def ensure_users_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS `users` (
                        uid CHAR(36) PRIMARY KEY,
                        apple_user_id VARCHAR(255) NOT NULL,
                        email VARCHAR(255) NULL,
                        email_is_private_relay TINYINT(1) NOT NULL DEFAULT 0,
                        display_name VARCHAR(120) NULL,
                        avatar_data MEDIUMBLOB NULL,
                        avatar_content_type VARCHAR(64) NULL,
                        avatar_updated_at DATETIME NULL,
                        membership_tier VARCHAR(32) NOT NULL DEFAULT 'free',
                        membership_status VARCHAR(32) NOT NULL DEFAULT 'inactive',
                        membership_product_id VARCHAR(128) NULL,
                        membership_expires_at DATETIME NULL,
                        last_sign_in_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY idx_apple_user_id (apple_user_id),
                        INDEX idx_updated_at (updated_at),
                        INDEX idx_membership_status (membership_status)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

    def upsert_apple_user(self, payload: dict) -> tuple[dict, bool]:
        apple_user_id = self._required_text(payload.get("appleUserId"), "appleUserId")
        proposed_uid = self._optional_text(payload.get("uid"), 36)
        email = self._optional_email(payload.get("email"))
        email_is_private_relay = bool(payload.get("emailIsPrivateRelay", False))
        display_name = self._optional_text(payload.get("displayName"), 120)
        avatar_bytes = self._decode_avatar(payload.get("avatarBase64"))
        avatar_content_type = self._optional_text(payload.get("avatarContentType"), 64)

        with self._connect() as conn:
            with conn.cursor() as cur:
                if proposed_uid:
                    cur.execute(
                        "SELECT uid, apple_user_id FROM `users` WHERE uid = %s LIMIT 1",
                        (proposed_uid,),
                    )
                    row = cur.fetchone()
                    if row is not None and row["apple_user_id"] != apple_user_id:
                        raise UserConflictError("uid 已绑定到其他 Apple 账号。")

                cur.execute(
                    "SELECT * FROM `users` WHERE apple_user_id = %s LIMIT 1",
                    (apple_user_id,),
                )
                existing = cur.fetchone()

                if existing is None:
                    uid = proposed_uid or str(uuid.uuid4())
                    cur.execute(
                        """
                        INSERT INTO `users` (
                            uid,
                            apple_user_id,
                            email,
                            email_is_private_relay,
                            display_name,
                            avatar_data,
                            avatar_content_type,
                            avatar_updated_at,
                            last_sign_in_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            uid,
                            apple_user_id,
                            email,
                            1 if email_is_private_relay else 0,
                            display_name,
                            avatar_bytes,
                            avatar_content_type,
                            datetime.now() if avatar_bytes is not None else None,
                        ),
                    )
                    created = True
                else:
                    uid = existing["uid"]
                    updates: list[str] = ["last_sign_in_at = NOW()"]
                    params: list[object] = []

                    if email is not None:
                        updates.append("email = %s")
                        params.append(email)
                        updates.append("email_is_private_relay = %s")
                        params.append(1 if email_is_private_relay else 0)
                    if display_name is not None:
                        updates.append("display_name = %s")
                        params.append(display_name)
                    if avatar_bytes is not None:
                        updates.append("avatar_data = %s")
                        params.append(avatar_bytes)
                        updates.append("avatar_content_type = %s")
                        params.append(avatar_content_type)
                        updates.append("avatar_updated_at = NOW()")

                    params.append(uid)
                    cur.execute(
                        f"UPDATE `users` SET {', '.join(updates)} WHERE uid = %s",
                        tuple(params),
                    )
                    created = False

                return self._fetch_user_by_uid(cur, uid), created

    def update_user_profile(self, uid: str, payload: dict) -> dict:
        uid = self._required_text(uid, "uid")
        apple_user_id = self._optional_text(payload.get("appleUserId"), 255)
        display_name = self._optional_text(payload.get("displayName"), 120)
        avatar_bytes = self._decode_avatar(payload.get("avatarBase64"))
        avatar_content_type = self._optional_text(payload.get("avatarContentType"), 64)

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM `users` WHERE uid = %s LIMIT 1",
                    (uid,),
                )
                row = cur.fetchone()
                if row is None:
                    raise UserNotFoundError("用户不存在。")
                if apple_user_id and row["apple_user_id"] != apple_user_id:
                    raise UserConflictError("appleUserId 与 uid 不匹配。")

                updates: list[str] = []
                params: list[object] = []
                if display_name is not None:
                    updates.append("display_name = %s")
                    params.append(display_name)
                if avatar_bytes is not None:
                    updates.append("avatar_data = %s")
                    params.append(avatar_bytes)
                    updates.append("avatar_content_type = %s")
                    params.append(avatar_content_type)
                    updates.append("avatar_updated_at = NOW()")

                if updates:
                    params.append(uid)
                    cur.execute(
                        f"UPDATE `users` SET {', '.join(updates)} WHERE uid = %s",
                        tuple(params),
                    )

                return self._fetch_user_by_uid(cur, uid)

    def _fetch_user_by_uid(self, cur, uid: str) -> dict:
        cur.execute(
            "SELECT * FROM `users` WHERE uid = %s LIMIT 1",
            (uid,),
        )
        row = cur.fetchone()
        if row is None:
            raise UserNotFoundError("用户不存在。")
        return self._serialize_user(row)

    def _serialize_user(self, row: dict) -> dict:
        return {
            "uid": row["uid"],
            "appleUserId": row["apple_user_id"],
            "email": row.get("email"),
            "emailIsPrivateRelay": bool(row.get("email_is_private_relay") or 0),
            "displayName": row.get("display_name"),
            "hasAvatar": row.get("avatar_data") is not None,
            "membershipTier": row.get("membership_tier") or "free",
            "membershipStatus": row.get("membership_status") or "inactive",
            "membershipProductId": row.get("membership_product_id"),
            "membershipExpiresAt": self._isoformat(row.get("membership_expires_at")),
            "lastSignInAt": self._isoformat(row.get("last_sign_in_at")),
            "createdAt": self._isoformat(row.get("created_at")),
            "updatedAt": self._isoformat(row.get("updated_at")),
        }

    def _isoformat(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _required_text(self, value, field_name: str) -> str:
        text = self._optional_text(value, 255)
        if text is None:
            raise UserValidationError(f"{field_name} 不能为空。")
        return text

    def _optional_text(self, value, max_length: int) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if len(text) > max_length:
            raise UserValidationError(f"字段长度超过限制: {max_length}")
        return text

    def _optional_email(self, value) -> str | None:
        text = self._optional_text(value, 255)
        if text is None:
            return None
        return text.lower()

    def _decode_avatar(self, value) -> bytes | None:
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        try:
            decoded = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise UserValidationError("头像数据不是合法的 base64。") from exc
        if len(decoded) > 2 * 1024 * 1024:
            raise UserValidationError("头像数据不能超过 2MB。")
        return decoded
