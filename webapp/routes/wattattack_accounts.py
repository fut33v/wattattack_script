from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status

from ..dependencies import require_admin
from repositories import wattattack_account_repository

log = logging.getLogger(__name__)

router = APIRouter(prefix="/wattattack/accounts", tags=["wattattack_accounts"], dependencies=[Depends(require_admin)])


def _serialize(account: dict) -> dict:
    return {
        "id": account.get("id"),
        "name": account.get("name"),
        "email": account.get("email"),
        "password": account.get("password"),
        "base_url": account.get("base_url"),
        "stand_ids": account.get("stand_ids") or [],
        "created_at": account.get("created_at").isoformat() if hasattr(account.get("created_at"), "isoformat") else None,
        "updated_at": account.get("updated_at").isoformat() if hasattr(account.get("updated_at"), "isoformat") else None,
    }


@router.get("")
def list_accounts():
    try:
        accounts = wattattack_account_repository.list_accounts()
        return {"items": [_serialize(acc) for acc in accounts]}
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to list WattAttack accounts")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось получить аккаунты") from exc


@router.post("")
def upsert_account(payload: dict):
    try:
        account_id = payload.get("id")
        email = payload.get("email")
        password = payload.get("password")
        if not account_id or not email or not password:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "id, email и password обязательны")

        stand_ids_raw = payload.get("stand_ids") or []
        stand_ids: Optional[list[int]] = None
        if isinstance(stand_ids_raw, list):
            clean: list[int] = []
            for value in stand_ids_raw:
                try:
                    clean.append(int(value))
                except (TypeError, ValueError):
                    continue
            stand_ids = clean

        account = wattattack_account_repository.upsert_account(
            account_id=str(account_id),
            name=payload.get("name"),
            email=email,
            password=password,
            base_url=payload.get("base_url"),
            stand_ids=stand_ids,
        )
        return {"item": _serialize(account)}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to upsert WattAttack account %s", payload.get("id"))
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось сохранить аккаунт") from exc


@router.delete("/{account_id}")
def delete_account(account_id: str):
    try:
        deleted = wattattack_account_repository.delete_account(account_id)
        if not deleted:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Аккаунт не найден")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        log.exception("Failed to delete WattAttack account %s", account_id)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Не удалось удалить аккаунт") from exc
