#!/usr/bin/env python3
"""PreToolUse hook: refuse Bash commands that use a dotenv file as a path.

Blocks commands where a dotenv file appears as its own shell word (an
argument, a redirection target, a sourced file), so `cat .env` or
`grep KEY config/.env.local` is refused. Mentions inside quoted strings
and heredoc bodies are allowed, so docs and commit messages can talk
about the file. This guards against accidental reads, not determined ones.
"""

import json
import os
import re
import shlex
import sys

# .env, .env.local, .env.production ... but not the committed templates.
DOTENV = re.compile(r"^\.env(\.[A-Za-z0-9_-]+)?$")
TEMPLATES = {".env.example", ".env.sample", ".env.template"}
HEREDOC = re.compile(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1")


def strip_heredoc_bodies(command: str) -> str:
    """Drop heredoc bodies, keeping the lines that start them."""
    out, delimiters = [], []
    for line in command.splitlines():
        if delimiters:
            if line.strip() == delimiters[0]:
                delimiters.pop(0)
            continue
        out.append(line)
        delimiters.extend(m.group(2) for m in HEREDOC.finditer(line))
    return "\n".join(out)


def references_dotenv(command: str) -> bool:
    try:
        lexer = shlex.shlex(strip_heredoc_bodies(command), posix=True,
                            punctuation_chars=True)
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        # Unbalanced quotes: fall back to plain whitespace splitting.
        tokens = command.split()
    for token in tokens:
        name = os.path.basename(token)
        if DOTENV.match(name) and name not in TEMPLATES:
            return True
    return False


def main() -> int:
    payload = json.load(sys.stdin)
    command = payload.get("tool_input", {}).get("command", "")
    if references_dotenv(command):
        print("Blocked: this command reads a .env file, which holds secrets.",
              file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
