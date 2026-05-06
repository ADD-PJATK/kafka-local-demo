from __future__ import annotations

import argparse
import socket
import sys

from kafka_demo_common import (
    add_common_flags,
    handle_introspection,
    is_docker_daemon_running,
    repo_root_from_this_file,
    require_tools,
    which_or_none,
)


def check_port(port: int) -> tuple[bool, str]:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(0.5)
        s.connect(("127.0.0.1", port))
        return False, f"Port {port} is already in use on 127.0.0.1"
    except Exception:
        return True, f"Port {port} is available"
    finally:
        try:
            s.close()
        except Exception:
            pass


def main() -> None:
    root_dir = repo_root_from_this_file(__file__)

    p = argparse.ArgumentParser(
        description="Setup check for the local Kafka demo (cross-platform).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_common_flags(p)
    p.add_argument("--check-ports", action="store_true", help="Check if Kafka/UI ports are available.")
    p.add_argument("--kafka-port", type=int, default=9092, help="Kafka external port to check.")
    p.add_argument("--ui-port", type=int, default=8089, help="Kafka UI port to check.")
    args = p.parse_args()

    handle_introspection(
        parser=p,
        module_file=__file__,
        functions=[check_port, main],
        args=args,
    )

    print("Kafka demo setup check")
    print(f"- Project root: {root_dir}")
    print(f"- Python: {sys.version.split()[0]}")

    require_tools(["docker"])
    print(f"- docker: {which_or_none('docker')}")

    from subprocess import run as _run

    cp = _run(["docker", "compose", "version"], cwd=str(root_dir), capture_output=True, text=True)
    if cp.returncode != 0:
        raise SystemExit(
            "Docker Compose (plugin) not available. Install Docker Desktop or a Docker Engine with the compose plugin.\n"
            f"Details:\n{cp.stderr or cp.stdout}"
        )
    print(f"- docker compose: OK ({(cp.stdout or '').strip()})")

    if not is_docker_daemon_running(root_dir):
        raise SystemExit("Docker daemon does not seem to be running. Start Docker Desktop / service and retry.")
    print("- docker daemon: running")

    if args.check_ports:
        ok, msg = check_port(args.kafka_port)
        print(f"- {msg}")
        if not ok:
            raise SystemExit(msg)
        ok, msg = check_port(args.ui_port)
        print(f"- {msg}")
        if not ok:
            raise SystemExit(msg)

    print("OK")


if __name__ == "__main__":
    main()

