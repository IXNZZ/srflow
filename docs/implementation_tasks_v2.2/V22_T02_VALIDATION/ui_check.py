#!/usr/bin/env python3
"""V22-T02 UI 夹具驱动：按 manifest 逐项编译并**严格匹配目标诊断**。

用法（工程根执行）：

```sh
cargo build --offline --lib
python3 docs/implementation_tasks_v2.2/V22_T02_VALIDATION/ui_check.py                # 发布清单：每项必须通过
python3 docs/implementation_tasks_v2.2/V22_T02_VALIDATION/ui_check.py --witnesses    # 见证清单：每项必须被判 FAIL
```

严格判定（`outcome=reject` 的每一行都必须同时满足）：

1. 编译必须失败；
2. 全部错误码 ⊆ `allowed`，且存在主诊断错误码 == `primary`；
3. `anchor` 是夹具内**唯一**一行源码标记；该错误码的**主诊断 span** 必须落在该行
   （span 的 `[line_start, line_end]` 与该行相交，且 `file_name` 解析为同一夹具）；
4. `symbols`（逗号分隔）每一项都必须出现在该诊断的 message 或该行源码文本中。

`anchor` 用源码标记而不是绝对行号，避免格式化导致漂移；同一错误码出现在别处的
无关失败（例如无关 E0277、另一个私有方法）不会通过。每行判定同时保存展开后的命令、
原始 JSON 诊断与匹配结果到 `evidence/ui_diagnostics/`。

`--witnesses` 用于反向见证：清单中的行必须被上述严格判定**拒绝**（即判 FAIL），
同时记录“仅比较错误码”的宽松判定会是 PASS，证明严格判定具备辨别力。

驱动只编译与核对诊断，不构建业务 runner，也不改动被测实现。
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
FIXTURES = ROOT / "tests" / "ui"
MANIFEST = HERE / "ui_manifest.tsv"
WITNESS_MANIFEST = HERE / "ui_manifest_witnesses.tsv"
RLIB = ROOT / "target" / "debug" / "libsrflow.rlib"
DEPS = ROOT / "target" / "debug" / "deps"
DIAG_DIR = HERE / "evidence" / "ui_diagnostics"
WITNESS_DIAG_DIR = HERE / "evidence" / "ui_diagnostics_witnesses"

COLUMNS = ["path", "mode", "outcome", "primary", "allowed", "anchor", "symbols", "acceptance", "note"]


def log(message: str) -> None:
    print(message, flush=True)


def read_manifest(path: Path) -> list[dict]:
    rows: list[dict] = []
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        parts = raw.split("\t")
        if parts[0] == "path":
            continue
        if len(parts) != len(COLUMNS):
            raise SystemExit(f"FATAL {path.name} 第 {number} 行列数不为 {len(COLUMNS)}：{raw!r}")
        row = dict(zip(COLUMNS, parts))
        if row["mode"] not in {"int", "ext"} or row["outcome"] not in {"ok", "reject"}:
            raise SystemExit(f"FATAL {path.name} 第 {number} 行 mode/outcome 非法：{raw!r}")
        row["allowed"] = {code for code in row["allowed"].split(",") if code and code != "-"}
        row["symbols"] = [token for token in row["symbols"].split(",") if token and token != "-"]
        rows.append(row)
    if not rows:
        raise SystemExit(f"FATAL {path.name} 没有任何行")
    return rows


def ensure_rlib() -> None:
    proc = subprocess.run(
        ["cargo", "build", "--offline", "--lib"], cwd=ROOT, capture_output=True, text=True
    )
    if proc.returncode != 0:
        raise SystemExit("FATAL cargo build --offline --lib 失败：\n" + proc.stderr)
    if not RLIB.is_file():
        raise SystemExit(f"FATAL 缺少 rlib：{RLIB}")


def compile_fixture(row: dict, tmp: Path) -> tuple[list[str], subprocess.CompletedProcess]:
    fixture = FIXTURES / row["path"]
    command = [
        "rustc",
        "--edition=2024",
        "--emit=metadata",
        "--error-format=json",
        str(fixture),
        "-o",
        str(tmp / f"{row['path']}.rmeta"),
    ]
    if row["mode"] == "ext":
        command += ["--extern", f"srflow={RLIB}", "-L", f"dependency={DEPS}"]
    proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    return command, proc


def parse_diagnostics(stderr: str) -> list[dict]:
    diagnostics = []
    for line in stderr.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("level") == "error" and (payload.get("code") or {}).get("code"):
            diagnostics.append(payload)
    return diagnostics


def anchor_line_of(fixture: Path, anchor: str) -> tuple[int, int]:
    """返回（匹配行号 1 基，匹配次数）。"""
    if not anchor:
        return 0, 0
    matches = [
        index
        for index, text in enumerate(
            fixture.read_text(encoding="utf-8").splitlines(), 1
        )
        if anchor in text
    ]
    return (matches[0] if matches else 0), len(matches)


def loose_ok(row: dict, proc: subprocess.CompletedProcess, diagnostics: list[dict]) -> bool:
    """仅比较错误码/允许集合的宽松判定（用于见证对照）。"""
    if row["outcome"] == "ok":
        return proc.returncode == 0
    codes = [(diag.get("code") or {}).get("code") for diag in diagnostics]
    return (
        proc.returncode != 0
        and row["primary"] in codes
        and set(codes) <= row["allowed"]
    )


def strict_ok(
    row: dict, proc: subprocess.CompletedProcess, diagnostics: list[dict]
) -> tuple[bool, str]:
    """严格判定：错误码 + 锚点行 + 符号。返回（是否通过，说明）。"""
    if row["outcome"] == "ok":
        return proc.returncode == 0, "expected successful compilation"
    if proc.returncode == 0:
        return False, "expected a compile failure"
    codes = [(diag.get("code") or {}).get("code") for diag in diagnostics]
    if not diagnostics:
        return False, "no coded error diagnostics"
    if row["primary"] not in codes:
        return False, f"primary code {row['primary']} missing (codes={codes})"
    if not set(codes) <= row["allowed"]:
        return False, f"unexpected codes outside allowed={sorted(row['allowed'])}: {codes}"

    fixture = FIXTURES / row["path"]
    anchor_line, anchor_hits = anchor_line_of(fixture, row["anchor"])
    if anchor_line == 0:
        return False, f"anchor marker not found in fixture: {row['anchor']!r}"
    if anchor_hits != 1:
        return False, f"anchor marker is not unique ({anchor_hits} matches): {row['anchor']!r}"
    anchor_text = fixture.read_text(encoding="utf-8").splitlines()[anchor_line - 1]

    candidates = [
        diag for diag in diagnostics if (diag.get("code") or {}).get("code") == row["primary"]
    ]
    for diag in candidates:
        for span in diag.get("spans", []):
            if not span.get("is_primary"):
                continue
            if Path(span.get("file_name", "")).name != fixture.name:
                continue
            start = span.get("line_start", 0)
            end = span.get("line_end", start)
            if not (start <= anchor_line <= end):
                continue
            haystack = diag.get("message", "") + "\n" + anchor_text
            missing = [token for token in row["symbols"] if token not in haystack]
            if missing:
                return False, f"diagnostic on anchor line lacks symbols {missing}"
            return True, f"{row['primary']} on line {anchor_line} with symbols {row['symbols']}"
    observed = [
        (
            (diag.get("code") or {}).get("code"),
            [
                (Path(span.get("file_name", "")).name, span.get("line_start"), span.get("line_end"))
                for span in diag.get("spans", [])
                if span.get("is_primary")
            ],
        )
        for diag in candidates
    ]
    return False, f"no {row['primary']} primary span on anchor line {anchor_line}: {observed}"


def run_row(row: dict, tmp: Path, diag_dir: Path) -> dict:
    command, proc = compile_fixture(row, tmp)
    diagnostics = parse_diagnostics(proc.stderr)
    ok, detail = strict_ok(row, proc, diagnostics)
    loose = loose_ok(row, proc, diagnostics)
    diag_dir.mkdir(parents=True, exist_ok=True)
    case = f"{row['mode']}_{row['path']}"
    (diag_dir / f"{case}.cmd").write_text(" ".join(command) + "\n", encoding="utf-8")
    (diag_dir / f"{case}.jsonl").write_text(proc.stderr, encoding="utf-8")
    (diag_dir / f"{case}.verdict").write_text(
        f"strict={'PASS' if ok else 'FAIL'}\nloose={'PASS' if loose else 'FAIL'}\n"
        f"detail={detail}\n",
        encoding="utf-8",
    )
    return {
        "row": row,
        "command": command,
        "exit": proc.returncode,
        "codes": [(diag.get("code") or {}).get("code") for diag in diagnostics],
        "ok": ok,
        "loose": loose,
        "detail": detail,
    }


def main() -> int:
    witness_mode = "--witnesses" in sys.argv[1:]
    manifest = WITNESS_MANIFEST if witness_mode else MANIFEST
    rows = read_manifest(manifest)
    log(f"manifest: {manifest.relative_to(ROOT)} rows: {len(rows)} mode={'witness' if witness_mode else 'publish'}")
    if any(row["mode"] == "ext" for row in rows):
        ensure_rlib()
    diag_dir = WITNESS_DIAG_DIR if witness_mode else DIAG_DIR
    if witness_mode:
        shutil.rmtree(diag_dir, ignore_errors=True)
    else:
        shutil.rmtree(diag_dir, ignore_errors=True)
    failures: list[str] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        for row in rows:
            result = run_row(row, tmp, diag_dir)
            if witness_mode:
                # 见证模式：严格判定必须拒绝；同时记录宽松判定会误判 PASS。
                verdict_ok = not result["ok"]
                log(
                    f"{'PASS' if verdict_ok else 'FAIL'} witness {row['path']} "
                    f"strict={'PASS' if result['ok'] else 'FAIL'} "
                    f"loose={'PASS' if result['loose'] else 'FAIL'} "
                    f"codes={result['codes']} detail={result['detail']}"
                )
                if not verdict_ok:
                    failures.append(row["path"])
                    log("  witness was NOT rejected by strict matching")
                elif not result["loose"]:
                    log("  note: loose matching also failed; witness does not prove discrimination")
                continue
            log(
                f"{'PASS' if result['ok'] else 'FAIL'} {row['path']} "
                f"[{row['mode']}/{row['acceptance']}] exit={result['exit']} "
                f"codes={result['codes']} detail={result['detail']}"
            )
            if not result["ok"]:
                failures.append(row["path"])
    log(f"rows: {len(rows)} failures: {len(failures)}")
    if failures:
        log("FAILED rows: " + ", ".join(failures))
        return 1
    log("all rows PASS" if not witness_mode else "all witnesses correctly rejected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
