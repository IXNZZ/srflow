#!/usr/bin/env python3
"""日常 UI 夹具驱动：按清单逐项编译/运行并**严格匹配目标诊断**。

判定口径沿用既有严格口径（错误码 + 允许集合 + 唯一锚点行 + 符号 token）：
  - `int`／`ext` 负例：必须失败，且主码、允许集合、主诊断 span 所在锚点行与符号全部命中；
  - `run`／`run-int` 正例：必须编译成功、可执行并让 stdout 命中 marker；
  - `consumer`：以独立 Cargo 工程（重命名依赖 + 派生宏）运行，命令与原始输出入证据；
  - `--witnesses`：同一锚点下**宽松判定必须误通过、严格判定必须拒绝**才算成立，
    用于证明严格判定确实比“只看错误码”更强。
  - `--self-test`：用真实夹具构造若干“应当被拒绝”的输入（意外成功、无关错误码、
    错误位置、错误符号、锚点缺失）与“应当被接受”的对照，验证判定器本身有区分力。

路径约定（本文件即日常入口）：
    python3 tests/support/ui_check.py
    python3 tests/support/ui_check.py --witnesses
    python3 tests/support/ui_check.py --self-test

原始诊断写入 `target/test-cleanup-evidence/`，不向 `docs/` 写任何文件。

本文件由 docs 内历史驱动（T07 校验目录，只读）按新位置改写：根目录按 `tests/support/`
重算，清单与夹具路径改为本目录与 `tests/ui/`，消费者改为 `tests/support/consumer/`，
诊断目录移出 docs。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FIXTURES = ROOT / "tests" / "ui"
EVIDENCE = ROOT / "target" / "test-cleanup-evidence"
TARGET_DIR = Path(os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")))
PROFILE_DIR = TARGET_DIR / "debug"
DEPS = PROFILE_DIR / "deps"
DIAG_DIR = EVIDENCE / "ui_diagnostics"
WITNESS_DIAG_DIR = EVIDENCE / "ui_diagnostics_witnesses"
CONSUMER = HERE / "consumer"
CONSUMER_TARGET = EVIDENCE / "consumer-target"

COLUMNS = [
    "case_id",
    "path",
    "mode",
    "outcome",
    "primary",
    "allowed",
    "anchor",
    "symbols",
    "marker",
    "note",
]
MODES = {"int", "ext", "run", "run-int", "consumer"}


def log(message: str) -> None:
    print(message, flush=True)


def read_manifest(path: Path) -> list[dict]:
    rows: list[dict] = []
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip() or raw.startswith("#"):
            continue
        parts = raw.rstrip("\n").split("\t")
        if parts[0] == "case_id":
            continue
        if len(parts) != len(COLUMNS):
            raise SystemExit(f"FATAL {path.name} 第 {number} 行列数不为 {len(COLUMNS)}：{raw!r}")
        row = dict(zip(COLUMNS, parts))
        if row["mode"] not in MODES:
            raise SystemExit(f"FATAL {path.name} 第 {number} 行 mode 非法：{raw!r}")
        if row["outcome"] not in {"ok", "reject"}:
            raise SystemExit(f"FATAL {path.name} 第 {number} 行 outcome 非法：{raw!r}")
        row["allowed"] = {code for code in row["allowed"].split(",") if code and code != "-"}
        row["symbols"] = [token for token in row["symbols"].split(",") if token and token != "-"]
        rows.append(row)
    if not rows:
        raise SystemExit(f"FATAL {path.name} 没有任何行")
    return rows


ARTIFACTS: dict[str, Path] = {}


def cargo_artifacts(arguments: list[str]) -> dict[str, Path]:
    """按 cargo 自己的 JSON 产物消息取路径，避免按文件名猜历史 rlib。"""
    command = ["cargo", *arguments, "--message-format=json"]
    proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        raise SystemExit("FATAL " + " ".join(command) + " 失败：\n" + proc.stderr)
    found: dict[str, Path] = {}
    for line in proc.stdout.splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("reason") != "compiler-artifact":
            continue
        target = payload.get("target", {})
        name = target.get("name", "")
        if name not in {"srflow", "futures"} or "lib" not in target.get("kind", []):
            continue
        for filename in payload.get("filenames", []):
            if filename.endswith(".rlib"):
                found.setdefault(name, Path(filename))
    return found


def prepare_artifacts(rows: list[dict]) -> None:
    """准备本次构建的正式 lib 与（需要的）dev-dependency 产物；缺产物不静默降级。"""
    needs_futures = any(row["mode"] in {"ext", "run"} for row in rows)
    found = cargo_artifacts(["build", "--offline", "--lib"])
    if needs_futures:
        found.update(cargo_artifacts(["build", "--offline", "--tests"]))
    if "srflow" not in found:
        raise SystemExit("FATAL 未取得 srflow rlib（cargo build --offline --lib）")
    if needs_futures and "futures" not in found:
        raise SystemExit(
            "FATAL 缺少 futures rlib：run/ext 夹具需要 dev-dependency 产物；"
            "驱动不静默省略 --extern futures"
        )
    ARTIFACTS.clear()
    ARTIFACTS.update(found)
    log(f"# 产物：srflow={ARTIFACTS['srflow']}")
    if "futures" in ARTIFACTS:
        log(f"# 产物：futures={ARTIFACTS['futures']}")


def rustc_command(row: dict, tmp: Path, *, emit_binary: bool) -> list[str]:
    fixture = FIXTURES / row["path"]
    if emit_binary:
        command = ["rustc", "--edition=2024", "--error-format=json", str(fixture), "-o", str(tmp / "fixture-bin")]
    else:
        command = [
            "rustc",
            "--edition=2024",
            "--emit=metadata",
            "--error-format=json",
            str(fixture),
            "-o",
            str(tmp / f"{Path(row['path']).name}.rmeta"),
        ]
    if row["mode"] in {"ext", "run"}:
        command += ["--extern", f"srflow={ARTIFACTS['srflow']}", "-L", f"dependency={DEPS}"]
        command += ["--extern", f"futures={ARTIFACTS['futures']}"]
    return command


def compile_row(row: dict, tmp: Path, *, emit_binary: bool = False) -> subprocess.CompletedProcess:
    command = rustc_command(row, tmp, emit_binary=emit_binary)
    return subprocess.run(command, cwd=ROOT, capture_output=True, text=True)


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
    if not anchor:
        return 0, 0
    matches = [
        index
        for index, text in enumerate(fixture.read_text(encoding="utf-8").splitlines(), 1)
        if anchor in text
    ]
    return (matches[0] if matches else 0), len(matches)


def loose_ok(row: dict, proc: subprocess.CompletedProcess) -> bool:
    """仅比较错误码/允许集合的宽松判定（用于见证对照）。"""
    if row["outcome"] == "ok":
        return proc.returncode == 0
    codes = [(diag.get("code") or {}).get("code") for diag in parse_diagnostics(proc.stderr)]
    return proc.returncode != 0 and row["primary"] in codes and set(codes) <= row["allowed"]


def strict_ok(row: dict, proc: subprocess.CompletedProcess, stdout: str = "") -> tuple[bool, str]:
    """严格判定：错误码 + 允许集合 + 唯一锚点行 + 符号 token（正例再查 marker）。"""
    if row["outcome"] == "ok":
        if proc.returncode != 0:
            return False, "expected successful compilation"
        if row["mode"] in {"run", "run-int", "consumer"} and row["marker"]:
            if row["marker"] not in stdout:
                return False, f"stdout missing marker {row['marker']!r}: {stdout[-200:]!r}"
        return True, "ok"
    if proc.returncode == 0:
        return False, "expected a compile failure"
    diagnostics = parse_diagnostics(proc.stderr)
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


def run_consumer(tmp: Path) -> tuple[list[str], subprocess.CompletedProcess]:
    """独立 Cargo 消费者：外置 target、`--locked`，不向 docs/ 写 lock 或缓存。"""
    command = [
        "cargo",
        "run",
        "--offline",
        "--quiet",
        "--locked",
        "--manifest-path",
        str(CONSUMER / "Cargo.toml"),
    ]
    environment = dict(os.environ)
    environment["CARGO_TARGET_DIR"] = str(CONSUMER_TARGET)
    proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, env=environment)
    return command, proc


def run_row(row: dict, tmp: Path, diag_dir: Path) -> dict:
    if row["mode"] == "consumer":
        manifest_path = CONSUMER / "Cargo.toml"
        if not manifest_path.is_file():
            return {"case_id": row["case_id"], "ok": False, "detail": f"缺少消费者工程 {manifest_path}"}
        command, proc = run_consumer(tmp)
        stdout = proc.stdout
    else:
        fixture = FIXTURES / row["path"]
        if not fixture.is_file():
            return {"case_id": row["case_id"], "ok": False, "detail": f"缺少夹具 {fixture}"}
        emit_binary = row["mode"] in {"run", "run-int"}
        command = rustc_command(row, tmp, emit_binary=emit_binary)
        proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        stdout = proc.stdout
        if proc.returncode == 0 and emit_binary:
            binary = tmp / "fixture-bin"
            run = subprocess.run([str(binary)], cwd=ROOT, capture_output=True, text=True)
            stdout = run.stdout
            proc = subprocess.CompletedProcess(proc.args, run.returncode, run.stdout, run.stderr)
    ok, detail = strict_ok(row, proc, stdout)
    diag_dir.mkdir(parents=True, exist_ok=True)
    body = [
        f"# command: {' '.join(command)}",
        f"# exit: {proc.returncode}",
        f"# strict: {'PASS' if ok else 'FAIL'} {detail}",
        "# ---- stderr ----",
        proc.stderr,
        "# ---- stdout ----",
        stdout,
    ]
    (diag_dir / f"{row['case_id']}.log").write_text("\n".join(body), encoding="utf-8")
    return {"case_id": row["case_id"], "ok": ok, "detail": detail}


def run_manifest(rows: list[dict], diag_dir: Path, *, witness_mode: bool) -> list[dict]:
    failures: list[dict] = []
    loose_only: list[str] = []
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp = Path(raw_tmp)
        for row in rows:
            fixture = FIXTURES / row["path"]
            if not fixture.is_file() and row["mode"] != "consumer":
                result = {"case_id": row["case_id"], "ok": False, "detail": f"缺少夹具 {fixture}"}
            elif witness_mode:
                emit_binary = row["mode"] in {"run", "run-int"}
                command = rustc_command(row, tmp, emit_binary=emit_binary)
                proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
                strict, detail = strict_ok(row, proc, "")
                loose = loose_ok(row, proc)
                # 见证语义：同一输入下宽松判定必须误通过、严格判定必须拒绝，二者同时成立才算 PASS。
                # 装配错误、无关错误码或意外编译成功都会使 loose 不成立（或 strict 成立），从而被拒绝。
                ok = loose and not strict
                if ok:
                    loose_only.append(row["case_id"])
                result = {
                    "case_id": row["case_id"],
                    "ok": ok,
                    "detail": f"strict={'PASS' if strict else 'FAIL'} loose={'PASS' if loose else 'FAIL'} | {detail}",
                }
                diag_dir.mkdir(parents=True, exist_ok=True)
                (diag_dir / f"{row['case_id']}.log").write_text(
                    "\n".join(
                        [
                            f"# command: {' '.join(command)}",
                            f"# exit: {proc.returncode}",
                            f"# strict: {'PASS' if strict else 'FAIL'} {detail}",
                            f"# loose: {'PASS' if loose else 'FAIL'}",
                            "# ---- stderr ----",
                            proc.stderr,
                        ]
                    ),
                    encoding="utf-8",
                )
            else:
                result = run_row(row, tmp, diag_dir)
            status = "PASS" if result["ok"] else "FAIL"
            if not result["ok"]:
                failures.append(result)
            log(f"[{status}] {result['case_id']}: {result['detail']}")
    log("")
    log(f"合计 {len(rows)} 项：通过 {len(rows) - len(failures)}，失败 {len(failures)}")
    if witness_mode:
        log(f"见证成立（宽松误通过 + 严格拒绝）: {loose_only}")
    for failure in failures:
        log(f"  FAIL {failure['case_id']}: {failure['detail']}")
    return failures


def self_test() -> int:
    """判定器自检：用真实夹具构造“必须被拒绝”的输入与“必须被接受”的对照。"""
    main_rows = read_manifest(HERE / "ui_manifest.tsv")
    witness_rows = read_manifest(HERE / "ui_manifest_witnesses.tsv")
    by_path = {row["path"]: row for row in main_rows}
    positive = by_path["positive__external_assembly.rs"]
    reject_row = by_path["borrow__blocks_destroy.rs"]
    witness_row = witness_rows[0]
    prepare_artifacts([positive, reject_row, witness_row])
    log(f"# 产物：srflow={ARTIFACTS['srflow']}")
    checks: list[tuple[str, bool, bool]] = []

    def mutate(base: dict, **changes) -> dict:
        row = dict(base)
        row.update(changes)
        return row

    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp = Path(raw_tmp)
        missing = [
            row["path"]
            for row in main_rows + witness_rows
            if row["mode"] != "consumer" and not (FIXTURES / row["path"]).is_file()
        ]
        checks.append(("清单夹具齐全", not missing, not missing))
        if missing:
            log(f"  FAIL 清单引用了不存在的夹具：{missing}")

        positive_proc = compile_row(positive, tmp)
        reject_proc = compile_row(reject_row, tmp)
        witness_proc = compile_row(witness_row, tmp)

        strict, _ = strict_ok(positive, positive_proc, "")
        checks.append(("正例按原判据应 PASS", strict, True))
        strict, _ = strict_ok(mutate(positive, outcome="reject"), positive_proc, "")
        checks.append(("意外编译成功必须被拒绝", strict, False))
        strict, _ = strict_ok(
            mutate(reject_row, primary="E0277", allowed={"E0277"}), reject_proc, ""
        )
        checks.append(("无关错误码必须被拒绝", strict, False))
        strict, _ = strict_ok(mutate(reject_row, allowed=set()), reject_proc, "")
        checks.append(("允许集合收窄后多余错误码必须被拒绝", strict, False))
        strict, loose = (
            lambda r: (strict_ok(r, reject_proc, "")[0], loose_ok(r, reject_proc))
        )(mutate(reject_row, anchor="fn main()"))
        checks.append(("错误位置必须被严格判定拒绝", strict, False))
        checks.append(("错误位置在宽松判定下会误通过（严格更严）", loose, True))
        strict, loose = (
            lambda r: (strict_ok(r, reject_proc, "")[0], loose_ok(r, reject_proc))
        )(mutate(reject_row, symbols=["NotARealSymbol"]))
        checks.append(("错误符号必须被严格判定拒绝", strict, False))
        checks.append(("错误符号在宽松判定下会误通过（严格更严）", loose, True))
        strict, loose = (
            lambda r: (strict_ok(r, reject_proc, "")[0], loose_ok(r, reject_proc))
        )(mutate(reject_row, anchor="__NO_SUCH_ANCHOR__"))
        checks.append(("锚点缺失必须被严格判定拒绝", strict, False))
        checks.append(("锚点缺失在宽松判定下会误通过（严格更严）", loose, True))

        w_strict, _ = strict_ok(witness_row, witness_proc, "")
        w_loose = loose_ok(witness_row, witness_proc)
        checks.append(("见证项：宽松误通过 + 严格拒绝", (w_loose and not w_strict), True))
        fake = mutate(witness_row, path=positive["path"])
        f_strict, _ = strict_ok(fake, positive_proc, "")
        f_loose = loose_ok(fake, positive_proc)
        checks.append(("伪装见证（指向可编译夹具）必须不成立", (f_loose and not f_strict), False))

    failures = 0
    for name, expected, got in checks:
        status = "PASS" if expected == got else "FAIL"
        if expected != got:
            failures += 1
        log(f"[{status}] 自检：{name}（期望 {'成立' if expected else '不成立'}）")
    log("")
    log(f"自检 {len(checks)} 项：通过 {len(checks) - failures}，失败 {failures}")
    return 1 if failures else 0


def main() -> int:
    witness_mode = "--witnesses" in sys.argv[1:]
    self_test_mode = "--self-test" in sys.argv[1:]
    manifest_arg = [a for a in sys.argv[1:] if a.startswith("--manifest=")]
    if self_test_mode:
        return self_test()
    if witness_mode:
        manifest = HERE / "ui_manifest_witnesses.tsv"
        diag_dir = WITNESS_DIAG_DIR
    else:
        manifest = HERE / "ui_manifest.tsv"
        diag_dir = DIAG_DIR
    if manifest_arg:
        manifest = Path(manifest_arg[0].split("=", 1)[1])
        if not manifest.is_absolute():
            manifest = ROOT / manifest
        diag_dir = manifest.parent / f"{manifest.stem}_diagnostics"
    rows = read_manifest(manifest)
    prepare_artifacts(rows)
    failures = run_manifest(rows, diag_dir, witness_mode=witness_mode)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
