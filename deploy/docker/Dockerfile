# Multi-stage, distroless-adjacent, runs as an unprivileged UID. One image, two
# roles: `plexus serve` for the control plane and `plexus worker` for the queue.
# Nothing but the interpreter and the wheel is installed, so the supply surface
# stays near zero (the core has no runtime dependencies).

FROM python:3.13-slim AS build
WORKDIR /build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
COPY pyproject.toml README.md ./
COPY src ./src
# Postgres is the production store; override with --build-arg PLEXUS_EXTRAS= for a
# sqlite-only image or =[observability] to bundle OTLP exporters.
ARG PLEXUS_EXTRAS=[postgres]
RUN pip wheel --wheel-dir /wheels ".${PLEXUS_EXTRAS}"

FROM python:3.13-slim AS runtime
LABEL org.opencontainers.image.title="plexus" \
      org.opencontainers.image.description="Multi-tenant MLOps/LLMOps control plane and inference gateway" \
      org.opencontainers.image.source="https://github.com/your-org/plexus"

RUN groupadd --gid 10001 plexus \
 && useradd --uid 10001 --gid 10001 --home-dir /var/lib/plexus --shell /usr/sbin/nologin plexus \
 && mkdir -p /var/lib/plexus && chown 10001:10001 /var/lib/plexus

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/var/lib/plexus

COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels plexus && rm -rf /wheels

USER 10001:10001
WORKDIR /var/lib/plexus
# Only used when no PLEXUS_DATABASE_URL is supplied; production must point at Postgres.
ENV PLEXUS_DATABASE_URL=sqlite:////var/lib/plexus/plexus.sqlite3 \
    PLEXUS_PORT=8080
EXPOSE 8080

# /healthz never touches a dependency, so it stays truthful during a Postgres outage
# (a dead process is killed; a degraded one just stops receiving traffic).
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import sys,urllib.request;sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).status==200 else 1)"]

ENTRYPOINT ["plexus"]
CMD ["serve"]
