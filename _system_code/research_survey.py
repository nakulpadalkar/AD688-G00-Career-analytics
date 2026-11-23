from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

try:  # Use a bundled copy first when this folder is distributed with a QMD.
    from . import research_survey_workbook as workbook
except ImportError:  # pragma: no cover - repo-local fallback
    try:
        from analytics import research_survey_workbook as workbook
    except ImportError:  # pragma: no cover - assignment folder fallback
        import research_survey_workbook as workbook


DEFAULT_ASSIGNMENT_CODE = "M01_A"
DEFAULT_OUTPUT_DIR = "research_submissions"


def assignment_code() -> str:
    workbook.load_dotenv_file()
    return os.environ.get("RESEARCH_ASSIGNMENT_CODE", DEFAULT_ASSIGNMENT_CODE).strip() or DEFAULT_ASSIGNMENT_CODE


def research_dir() -> Path:
    workbook.load_dotenv_file()
    return Path(os.environ.get("RESEARCH_SUBMISSION_DIR", DEFAULT_OUTPUT_DIR))


def print_runtime_summary() -> dict:
    report = workbook.runtime_report()
    print("Operating System:", report.get("operating_system", ""))
    print("Python Version:", report.get("python_version", ""))
    print("Machine:", report.get("machine", ""))
    print("Processor:", report.get("processor", ""))
    if "memory_total_mb" in report:
        print("Total Memory (MB):", report["memory_total_mb"])
    if "memory_available_mb" in report:
        print("Available Memory (MB):", report["memory_available_mb"])
    return report


def prepare_workbook() -> Path:
    code = assignment_code()
    output_dir = research_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    survey, survey_url = workbook.fetch_survey(code)
    repo_context = repository_context()
    existing = existing_workbook_path(code, output_dir)
    if existing and not force_new_workbook():
        matches, reason = workbook.workbook_matches_survey(existing, survey)
        if not matches:
            print(f"Existing workbook is stale ({reason}). Creating a fresh workbook.")
        else:
            print(f"Using existing workbook: {existing}")
            print(f"Workbook check: {reason}.")
            return existing
    elif existing:
        print("RESEARCH_FORCE_NEW_WORKBOOK is set. Creating a fresh workbook.")

    workbook_path = timestamped_workbook_path(code, output_dir, survey, repo_context)
    workbook.create_workbook(survey, workbook_path, force=False, **repo_context)
    pointer_path(code, output_dir).write_text(str(workbook_path), encoding="utf-8")
    print(f"Created workbook: {workbook_path}")
    print(f"Survey source: {survey_url}")
    if repo_context.get("repository_user_hint"):
        print(f"Repository user hint: {repo_context['repository_user_hint']}")
    return workbook_path


def force_new_workbook() -> bool:
    return os.environ.get("RESEARCH_FORCE_NEW_WORKBOOK", "").strip().lower() in {"1", "true", "yes", "y"}


def repository_context() -> dict:
    repository_name = os.environ.get("RESEARCH_REPOSITORY_NAME", "").strip() or Path.cwd().name
    repository_user_hint = os.environ.get("RESEARCH_STUDENT_GITHUB_USERNAME", "").strip() or user_hint_from_repository(repository_name)
    return {"repository_name": repository_name, "repository_user_hint": repository_user_hint}


def user_hint_from_repository(repository_name: str) -> str:
    normalized = str(repository_name or "").strip().lower()
    match = re.search(r"(?:^|-)m\d{1,2}-a-(?P<user>[a-z0-9][a-z0-9-]*)$", normalized)
    if match:
        return match.group("user")
    return ""


def validate_workbook(workbook_path: str | Path | None = None) -> Path:
    code = assignment_code()
    output_dir = research_dir()
    resolved_workbook = Path(workbook_path) if workbook_path else existing_workbook_path(code, output_dir)
    if not resolved_workbook:
        raise FileNotFoundError(
            f"No research survey workbook .xlsx file was found in {output_dir}. "
            f"{pointer_path(code, output_dir).name} is only a pointer file; rerun prepare_workbook() "
            "or pass the workbook path directly to validate_workbook(...)."
        )
    if not resolved_workbook.exists():
        raise FileNotFoundError(f"Research survey workbook not found: {resolved_workbook}")

    output_json = resolved_workbook.with_name(f"{resolved_workbook.stem}_validation.json")
    notebook_run_id = os.environ.get("NOTEBOOK_RUN_ID", resolved_workbook.stem)
    workbook.validate_workbook(
        resolved_workbook,
        assignment_code=code,
        notebook_run_id=notebook_run_id,
        output_json=output_json,
    )
    print("Workbook validation passed. No data was submitted to the course API or Django database.")
    print(f"Local validation report: {output_json}")
    return output_json


def existing_workbook_path(code: str | None = None, output_dir: str | Path | None = None) -> Path | None:
    code = code or assignment_code()
    output_dir = Path(output_dir) if output_dir else research_dir()
    pointer = pointer_path(code, output_dir)
    if pointer.exists():
        candidate = workbook_from_pointer(pointer, output_dir)
        if candidate:
            return candidate
        return None

    candidates = workbook_candidates(code, output_dir)
    if candidates:
        latest = candidates[-1]
        pointer.parent.mkdir(parents=True, exist_ok=True)
        pointer.write_text(str(latest), encoding="utf-8")
        return latest
    return None


def workbook_from_pointer(pointer: Path, output_dir: Path) -> Path | None:
    raw_value = pointer.read_text(encoding="utf-8").strip().strip('"').strip("'")
    if not raw_value:
        return None

    raw_path = Path(raw_value).expanduser()
    candidates = [raw_path]
    if not raw_path.is_absolute():
        candidates.extend(
            [
                pointer.parent / raw_path,
                output_dir / raw_path,
                pointer.parent / raw_path.name,
                output_dir / raw_path.name,
            ]
        )

    for candidate in dedupe_paths(candidates):
        if candidate.suffix.lower() == ".xlsx" and candidate.exists():
            return candidate
    return None


def workbook_candidates(code: str, output_dir: Path) -> list[Path]:
    slug = safe_slug(code)
    candidates = [output_dir / f"{slug}_response_workbook.xlsx"]
    candidates.extend(output_dir.glob(f"{slug}_response_workbook_*.xlsx"))
    existing = [path for path in dedupe_paths(candidates) if path.exists()]
    return sorted(existing, key=lambda path: (path.stat().st_mtime, path.name))


def dedupe_paths(paths) -> list[Path]:
    seen = set()
    unique = []
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def timestamped_workbook_path(
    code: str | None = None,
    output_dir: str | Path | None = None,
    survey_payload: dict | None = None,
    repo_context: dict | None = None,
) -> Path:
    code = code or assignment_code()
    output_dir = Path(output_dir) if output_dir else research_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parts = [safe_slug(code), "response_workbook"]
    if survey_payload:
        version = survey_payload.get("instrument_version") or (survey_payload.get("meta") or {}).get("instrument_version") or ""
        if version:
            parts.append(safe_slug(version))
    repo_context = repo_context or {}
    if repo_context.get("repository_user_hint"):
        parts.append(safe_slug(repo_context["repository_user_hint"]))
    parts.append(timestamp)
    return output_dir / f"{'_'.join(parts)}.xlsx"


def pointer_path(code: str, output_dir: Path) -> Path:
    return output_dir / f"{safe_slug(code)}_current_workbook.txt"


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return slug.strip("._-") or DEFAULT_ASSIGNMENT_CODE
