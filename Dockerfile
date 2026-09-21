# Single-architecture build: this app targets aarch64 (Raspberry Pi) only.
# build.yaml is deprecated; base image configuration belongs here.
FROM ghcr.io/home-assistant/aarch64-base-python:3.13-alpine3.23

# Install into the system prefix rather than a venv so the entrypoint can
# simply run python3. Never download a Python interpreter: use the image's.
ENV UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir uv

WORKDIR /app

# Dependencies first so edits to the monitor source do not invalidate this
# layer. --no-default-groups excludes dev/tools/notebooks: the image gets
# requests, httpx, python-dotenv and supabase, not the scientific stack.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups

COPY main.py health.py notify.py ./

COPY addon-run.sh /addon-run.sh
RUN chmod a+x /addon-run.sh

CMD [ "/addon-run.sh" ]
