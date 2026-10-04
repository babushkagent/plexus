"""Typed, validated configuration. Everything comes from the environment so the
same image runs on a laptop, in compose, and on Kubernetes (12-factor / WKS).

Configuration is validated eagerly: a misconfigured pod must die at startup, not
serve traffic with a silent default.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, replace
from enum import Enum
from typing import Any

DEFAULT_JWT_SECRET = "dev-only-insecure-secret-change-me"


class ConfigError(Exception):
    def __init__(self, problems: Sequence[str]) -> None:
        self.problems = list(problems)
        super().__init__("invalid configuration: " + "; ".join(self.problems))


class Environment(str, Enum):
    DEV = "dev"
    TEST = "test"
    STAGING = "staging"
    PROD = "prod"

    @property
    def is_production(self) -> bool:
        return self in (Environment.STAGING, Environment.PROD)


def _parse_dotenv(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            out[key.strip()] = value.strip().strip('"').strip("'")
    return out


class _Reader:
    def __init__(self, data: Mapping[str, str]) -> None:
        self._data = dict(data)

    def string(self, key: str, default: str) -> str:
        return self._data.get(key, default)

    def required(self, key: str, problems: list[str]) -> str:
        value = self._data.get(key, "").strip()
        if not value:
            problems.append(f"{key} is required")
        return value

    def integer(self, key: str, default: int, problems: list[str], *, minimum: int | None = None) -> int:
        raw = self._data.get(key)
        if raw is None or not raw.strip():
            return default
        try:
            value = int(raw)
        except ValueError:
            problems.append(f"{key} must be an integer (got {raw!r})")
            return default
        if minimum is not None and value < minimum:
            problems.append(f"{key} must be >= {minimum} (got {value})")
        return value

    def number(self, key: str, default: float, problems: list[str], *, low: float = 0.0) -> float:
        raw = self._data.get(key)
        if raw is None or not raw.strip():
            return default
        try:
            value = float(raw)
        except ValueError:
            problems.append(f"{key} must be a number (got {raw!r})")
            return default
        if value < low:
            problems.append(f"{key} must be >= {low} (got {value})")
        return value

    def boolean(self, key: str, default: bool) -> bool:
        raw = self._data.get(key)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def csv(self, key: str, default: Sequence[str]) -> tuple[str, ...]:
        raw = self._data.get(key, "").strip()
        if not raw:
            return tuple(default)
        return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class Settings:
    env: Environment = Environment.DEV
    service_name: str = "plexus"
    log_level: str = "INFO"
    json_logs: bool = True

    database_url: str = "sqlite:///var/lib/plexus/plexus.sqlite3"
    db_pool_size: int = 8
    db_statement_timeout_ms: int = 5_000

    jwt_issuer: str = "plexus"
    jwt_audience: str = "plexus.control-plane"
    jwt_secret: str = DEFAULT_JWT_SECRET
    jwks_url: str = ""
    jwt_leeway_s: int = 30
    api_key_pepper: str = ""

    host: str = "0.0.0.0"
    port: int = 8080
    request_timeout_s: float = 60.0
    max_body_bytes: int = 1_048_576
    shutdown_grace_s: float = 30.0

    default_rps: float = 50.0
    default_burst: float = 100.0
    max_concurrent_per_tenant: int = 32
    max_input_chars: int = 200_000

    worker_concurrency: int = 4
    task_lease_s: float = 60.0
    task_heartbeat_s: float = 15.0
    task_max_attempts: int = 5
    retry_base_delay_s: float = 0.5
    retry_max_delay_s: float = 120.0

    breaker_failure_ratio: float = 0.5
    breaker_min_calls: int = 10
    breaker_window_s: float = 30.0
    breaker_open_s: float = 20.0
    breaker_half_open_max: int = 3

    api_min_replicas: int = 2
    api_max_replicas: int = 40
    target_queue_depth: int = 64
    target_p95_ms: float = 800.0
    stabilize_up_s: float = 30.0
    stabilize_down_s: float = 300.0

    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: str = ""
    ollama_base_url: str = "http://127.0.0.1:11434"
    provider_timeout_s: float = 60.0
    default_model: str = "gpt-4o-mini"
    provider_max_attempts: int = 3

    allowed_models: tuple[str, ...] = ("gpt-4o-mini", "llama3.1:8b", "echo")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        dotenv: str | None = ".env",
    ) -> Settings:
        data = {**(dict(_parse_dotenv(dotenv)) if dotenv else {}), **(dict(os.environ) if environ is None else dict(environ))}
        r = _Reader(data)
        d = cls()
        problems: list[str] = []

        env_name = r.string("PLEXUS_ENV", "dev").lower()
        try:
            env = Environment(env_name)
        except ValueError:
            problems.append(f"PLEXUS_ENV must be one of {[e.value for e in Environment]} (got {env_name!r})")
            env = Environment.DEV

        settings = cls(
            env=env,
            service_name=r.string("PLEXUS_SERVICE_NAME", "plexus"),
            log_level=r.string("PLEXUS_LOG_LEVEL", "DEBUG" if env is Environment.DEV else "INFO").upper(),
            json_logs=r.boolean("PLEXUS_JSON_LOGS", True),
            database_url=r.string("PLEXUS_DATABASE_URL", d.database_url),
            db_pool_size=r.integer("PLEXUS_DB_POOL_SIZE", 8, problems, minimum=1),
            db_statement_timeout_ms=r.integer("PLEXUS_DB_STATEMENT_TIMEOUT_MS", 5_000, problems, minimum=100),
            jwt_issuer=r.string("PLEXUS_JWT_ISSUER", d.jwt_issuer),
            jwt_audience=r.string("PLEXUS_JWT_AUDIENCE", d.jwt_audience),
            jwt_secret=r.string("PLEXUS_JWT_SECRET", d.jwt_secret),
            jwks_url=r.string("PLEXUS_JWKS_URL", ""),
            jwt_leeway_s=r.integer("PLEXUS_JWT_LEEWAY_S", 30, problems, minimum=0),
            api_key_pepper=r.string("PLEXUS_API_KEY_PEPPER", ""),
            host=r.string("PLEXUS_HOST", d.host),
            port=r.integer("PLEXUS_PORT", 8080, problems, minimum=1),
            request_timeout_s=r.number("PLEXUS_REQUEST_TIMEOUT_S", 60.0, problems, low=0.1),
            max_body_bytes=r.integer("PLEXUS_MAX_BODY_BYTES", 1_048_576, problems, minimum=1024),
            shutdown_grace_s=r.number("PLEXUS_SHUTDOWN_GRACE_S", 30.0, problems, low=0.0),
            default_rps=r.number("PLEXUS_DEFAULT_RPS", 50.0, problems, low=0.001),
            default_burst=r.number("PLEXUS_DEFAULT_BURST", 100.0, problems, low=0.001),
            max_concurrent_per_tenant=r.integer("PLEXUS_MAX_CONCURRENT_PER_TENANT", 32, problems, minimum=1),
            max_input_chars=r.integer("PLEXUS_MAX_INPUT_CHARS", 200_000, problems, minimum=1),
            worker_concurrency=r.integer("PLEXUS_WORKER_CONCURRENCY", 4, problems, minimum=1),
            task_lease_s=r.number("PLEXUS_TASK_LEASE_S", 60.0, problems, low=1.0),
            task_heartbeat_s=r.number("PLEXUS_TASK_HEARTBEAT_S", 15.0, problems, low=0.1),
            task_max_attempts=r.integer("PLEXUS_TASK_MAX_ATTEMPTS", 5, problems, minimum=1),
            retry_base_delay_s=r.number("PLEXUS_RETRY_BASE_DELAY_S", 0.5, problems, low=0.0),
            retry_max_delay_s=r.number("PLEXUS_RETRY_MAX_DELAY_S", 120.0, problems, low=0.0),
            breaker_failure_ratio=r.number("PLEXUS_BREAKER_FAILURE_RATIO", 0.5, problems, low=0.01),
            breaker_min_calls=r.integer("PLEXUS_BREAKER_MIN_CALLS", 10, problems, minimum=1),
            breaker_window_s=r.number("PLEXUS_BREAKER_WINDOW_S", 30.0, problems, low=1.0),
            breaker_open_s=r.number("PLEXUS_BREAKER_OPEN_S", 20.0, problems, low=0.1),
            breaker_half_open_max=r.integer("PLEXUS_BREAKER_HALF_OPEN_MAX", 3, problems, minimum=1),
            api_min_replicas=r.integer("PLEXUS_API_MIN_REPLICAS", 2, problems, minimum=1),
            api_max_replicas=r.integer("PLEXUS_API_MAX_REPLICAS", 40, problems, minimum=1),
            target_queue_depth=r.integer("PLEXUS_TARGET_QUEUE_DEPTH", 64, problems, minimum=1),
            target_p95_ms=r.number("PLEXUS_TARGET_P95_MS", 800.0, problems, low=1.0),
            stabilize_up_s=r.number("PLEXUS_STABILIZE_UP_S", 30.0, problems, low=0.0),
            stabilize_down_s=r.number("PLEXUS_STABILIZE_DOWN_S", 300.0, problems, low=0.0),
            openai_base_url=r.string("OPENAI_BASE_URL", d.openai_base_url).rstrip("/"),
            openai_api_key=r.string("OPENAI_API_KEY", ""),
            ollama_base_url=r.string("OLLAMA_BASE_URL", d.ollama_base_url).rstrip("/"),
            provider_timeout_s=r.number("PLEXUS_PROVIDER_TIMEOUT_S", 60.0, problems, low=1.0),
            default_model=r.string("PLEXUS_DEFAULT_MODEL", d.default_model),
            provider_max_attempts=r.integer("PLEXUS_PROVIDER_MAX_ATTEMPTS", 3, problems, minimum=1),
            allowed_models=r.csv("PLEXUS_ALLOWED_MODELS", d.allowed_models),
        )

        if not settings.database_url.startswith(("sqlite:///", "postgresql://", "postgres://")):
            problems.append("PLEXUS_DATABASE_URL must use sqlite:/// or postgresql://")
        if settings.breaker_failure_ratio >= 1.0:
            problems.append("PLEXUS_BREAKER_FAILURE_RATIO must be < 1")
        if settings.task_heartbeat_s * 2 >= settings.task_lease_s:
            problems.append("PLEXUS_TASK_HEARTBEAT_S must be well below PLEXUS_TASK_LEASE_S")
        if settings.api_min_replicas > settings.api_max_replicas:
            problems.append("PLEXUS_API_MIN_REPLICAS must be <= PLEXUS_API_MAX_REPLICAS")
        if env.is_production:
            if settings.jwt_secret == DEFAULT_JWT_SECRET or len(settings.jwt_secret) < 32:
                problems.append("PLEXUS_JWT_SECRET must be a strong secret (>=32 chars) outside dev/test")
            if not settings.api_key_pepper:
                problems.append("PLEXUS_API_KEY_PEPPER is required outside dev/test")
            if settings.database_url.startswith("sqlite"):
                problems.append("sqlite is not supported in production; use postgresql://")
        if problems:
            raise ConfigError(problems)
        return settings

    def redacted(self) -> dict[str, Any]:
        secrets = {"jwt_secret", "api_key_pepper", "openai_api_key"}
        masked = {f.name: ("***" if f.name in secrets else getattr(self, f.name)) for f in fields(self)}
        return {k: (v.value if isinstance(v, Environment) else str(v)) for k, v in masked.items()}

    def with_overrides(self, **kwargs: Any) -> Settings:
        return replace(self, **kwargs)


_OVERRIDE_ENV = {
    "openai_api_key": "OPENAI_API_KEY",
    "openai_base_url": "OPENAI_BASE_URL",
    "ollama_base_url": "OLLAMA_BASE_URL",
}


def _env_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        return ",".join(str(item) for item in value)
    return str(value)


def load_settings(
    environ: Mapping[str, str] | None = None,
    *,
    dotenv: str | None = ".env",
    **overrides: Any,
) -> Settings:
    """Build settings from the environment, with typed keyword overrides layered on top.

    Overrides are folded into the environment *before* parsing so there is exactly one
    validation path: a setting that is illegal in prod is rejected here too.
    """
    merged = dict(environ) if environ is not None else dict(os.environ)
    for name, value in overrides.items():
        if not any(f.name == name for f in fields(Settings)):
            raise ConfigError([f"unknown setting {name!r}"])
        merged[_OVERRIDE_ENV.get(name, f"PLEXUS_{name.upper()}")] = _env_value(value)
    return Settings.from_env(merged, dotenv=dotenv)
