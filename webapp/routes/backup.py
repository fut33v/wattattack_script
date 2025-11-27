"""Backup import endpoints."""
from __future__ import annotations

import gzip
import logging
import os
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from ..dependencies import require_admin

log = logging.getLogger(__name__)

router = APIRouter(prefix="/backup", tags=["backup"], dependencies=[Depends(require_admin)])

MAX_UPLOAD_BYTES = int(os.environ.get("BACKUP_IMPORT_MAX_BYTES", str(512 * 1024 * 1024)))  # 512 MB default


def _build_db_url() -> str:
  """Build database URL from env, falling back to DB_* variables."""
  direct = os.environ.get("DATABASE_URL")
  if direct:
    return direct

  host = os.environ.get("DB_HOST", "localhost")
  port = os.environ.get("DB_PORT", "5432")
  name = os.environ.get("DB_NAME", "wattattack")
  user = os.environ.get("DB_USER", "wattattack")
  password = os.environ.get("DB_PASSWORD", "wattattack")

  user_part = quote_plus(user) if user else ""
  password_part = f":{quote_plus(password)}" if password else ""
  return f"postgresql://{user_part}{password_part}@{host}:{port}/{name}"


def _save_upload_to_temp(upload: UploadFile, *, temp_dir: Path) -> Path:
  target_name = upload.filename or "backup.sql"
  target_path = temp_dir / target_name

  try:
    upload.file.seek(0)
  except Exception:
    pass

  with target_path.open("wb") as handle:
    while True:
      chunk = upload.file.read(1024 * 1024)
      if not chunk:
        break
      handle.write(chunk)

  return target_path


def _is_gzip(path: Path) -> bool:
  try:
    with path.open("rb") as handle:
      return handle.read(2) == b"\x1f\x8b"
  except Exception:
    return False


def _prepare_sql_file(source_path: Path, *, workdir: Path) -> Path:
  if _is_gzip(source_path):
    target_name = source_path.stem if source_path.suffix.lower() in {".gz", ".gzip"} else f"{source_path.name}.sql"
    target_path = workdir / target_name
    with gzip.open(source_path, "rb") as gz_handle, target_path.open("wb") as dest_handle:
      shutil.copyfileobj(gz_handle, dest_handle)
    return target_path

  return source_path


def _run_psql(sql_path: Path) -> Tuple[int, str, str]:
  db_url = _build_db_url()
  cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-f", str(sql_path)]
  try:
    completed = subprocess.run(
      cmd,
      check=False,
      capture_output=True,
      text=True,
      env={**os.environ},
    )
  except FileNotFoundError as exc:  # pragma: no cover - runtime dependency
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="psql is not installed in the environment; install postgresql-client inside the app container.",
    ) from exc

  return completed.returncode, completed.stdout or "", completed.stderr or ""


@router.post("/import")
def import_backup(file: UploadFile = File(...)):
  """Import a pg_dump (.sql or .sql.gz) into the configured database."""
  started = time.monotonic()
  temp_dir = Path(tempfile.mkdtemp(prefix="backup_import_"))
  target_sql = None

  try:
    stored_path = _save_upload_to_temp(file, temp_dir=temp_dir)
    file_size = stored_path.stat().st_size
    if file_size > MAX_UPLOAD_BYTES:
      raise HTTPException(
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        detail=f"Файл слишком большой ({file_size} байт). Лимит {MAX_UPLOAD_BYTES} байт.",
      )

    target_sql = _prepare_sql_file(stored_path, workdir=temp_dir)
    applied_size = target_sql.stat().st_size
    log.info("Starting DB import from %s (%s bytes)", target_sql, applied_size)

    code, stdout, stderr = _run_psql(target_sql)
    duration_ms = int((time.monotonic() - started) * 1000)

    if code != 0:
      log.error("DB import failed (code=%s): %s", code, stderr.strip() or stdout.strip())
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail={
          "message": "Импорт не выполнился, проверьте лог.",
          "stdout": stdout[-4000:],
          "stderr": stderr[-4000:],
          "exit_code": code,
        },
      )

    log.info("DB import finished in %sms", duration_ms)
    return {
      "status": "ok",
      "restored_at": datetime.now(timezone.utc).isoformat(),
      "applied_bytes": applied_size,
      "duration_ms": duration_ms,
      "stdout": stdout[-4000:],
      "stderr": stderr[-4000:],
      "database_url_source": "DATABASE_URL" if os.environ.get("DATABASE_URL") else "DB_*",
      "filename": file.filename,
    }
  finally:
    try:
      file.file.close()
    except Exception:
      pass
    try:
      shutil.rmtree(temp_dir)
    except Exception:
      log.warning("Failed to cleanup temp dir %s", temp_dir)
