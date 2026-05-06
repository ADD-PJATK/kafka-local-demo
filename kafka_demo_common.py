from __future__ import annotations

import argparse
import inspect
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence


DEFAULT_TOPIC = "demo-events"
DEFAULT_GROUP = "demo-group"


@dataclass(frozen=True)
class Ctx:
    root_dir: Path
    show_code: bool


def repo_root_from_this_file(this_file: str) -> Path:
    # All CLI scripts live in the repo root.
    return Path(this_file).resolve().parent


def which_or_none(cmd: str) -> Optional[str]:
    return shutil.which(cmd)


def run(
    argv: Sequence[str],
    *,
    cwd: Path,
    check: bool = True,
    capture: bool = False,
    text: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(argv),
        cwd=str(cwd),
        check=check,
        capture_output=capture,
        text=text,
        input=input_text,
    )


def print_cmd(label: str, argv: Sequence[str]) -> None:
    pretty = " ".join(_shell_quote(a) for a in argv)
    print(f"[code] {label}:\n  {pretty}")


def _shell_quote(s: str) -> str:
    # Human-readable quoting for printouts (not for execution).
    if not s:
        return "''"
    if any(c.isspace() or c in "\"'\\$`" for c in s):
        return "'" + s.replace("'", "'\"'\"'") + "'"
    return s


def docker_compose(ctx: Ctx, args: Sequence[str], *, label: str, capture: bool = False) -> str:
    argv = ["docker", "compose", *args]
    if ctx.show_code:
        print_cmd(label, argv)
    cp = run(argv, cwd=ctx.root_dir, check=True, capture=capture, text=True)
    return cp.stdout if capture else ""


def docker_exec_kafka(ctx: Ctx, bash_cmd: str, *, label: str, capture: bool = False) -> str:
    argv = ["docker", "compose", "exec", "-T", "kafka", "bash", "-lc", bash_cmd]
    if ctx.show_code:
        print_cmd(label, argv)
    cp = run(argv, cwd=ctx.root_dir, check=True, capture=capture, text=True)
    return cp.stdout if capture else ""


def add_common_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("--no-show-code", action="store_true", help="Do not print underlying commands.")
    p.add_argument("--show-script", action="store_true", help="Print the full script source and exit.")
    p.add_argument(
        "--show-fn",
        metavar="NAME",
        help="Print source of a specific Python function (by name) and exit.",
    )
    p.add_argument("--show-fn-all", action="store_true", help="Print all function sources and exit.")


def handle_introspection(
    *,
    parser: argparse.ArgumentParser,
    module_file: str,
    functions: Iterable[Callable[..., object]],
    args: argparse.Namespace,
) -> None:
    if getattr(args, "show_script", False):
        print(Path(module_file).read_text(encoding="utf-8"))
        raise SystemExit(0)

    fn_map = {f.__name__: f for f in functions}

    if getattr(args, "show_fn_all", False):
        for name in sorted(fn_map):
            print(f"\n[code] def {name}(...):")
            print(inspect.getsource(fn_map[name]).rstrip())
        raise SystemExit(0)

    req = getattr(args, "show_fn", None)
    if req:
        if req not in fn_map:
            parser.error(f"Unknown function for --show-fn: {req}. Available: {', '.join(sorted(fn_map))}")
        print(inspect.getsource(fn_map[req]).rstrip())
        raise SystemExit(0)


def require_tools(tools: Sequence[str]) -> None:
    missing = [t for t in tools if which_or_none(t) is None]
    if missing:
        raise SystemExit(f"Missing required tools in PATH: {', '.join(missing)}")


def is_docker_daemon_running(root_dir: Path) -> bool:
    try:
        run(["docker", "info"], cwd=root_dir, check=False, capture=True)
        return True
    except Exception:
        return False

