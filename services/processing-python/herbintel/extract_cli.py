"""One document per disposable parser process, with Linux CPU/address-space limits."""

import json
import subprocess
import sys

from .extraction import extract


def main():
    if sys.platform == "linux":
        import resource

        resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024, 768 * 1024 * 1024))
        resource.setrlimit(resource.RLIMIT_CPU, (100, 100))
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
    try:
        result = extract(sys.stdin.buffer.read(8 * 1024 * 1024 + 1), sys.argv[1])
        sys.stdout.buffer.write(json.dumps(result, ensure_ascii=False).encode("utf-8"))
    except Exception as error:
        sys.stderr.write(type(error).__name__)
        permanent = isinstance(error, (ValueError, UnicodeError)) or type(error).__module__.startswith(
            ("pypdf", "zipfile", "docx")
        )
        raise SystemExit(3 if isinstance(error, (OSError, subprocess.SubprocessError)) or not permanent else 2)


if __name__ == "__main__":
    main()
