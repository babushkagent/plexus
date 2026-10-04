"""Provider abstraction for the inference gateway.

Every upstream (hosted OpenAI-compatible endpoint, local Ollama, a deterministic
echo used in tests) implements the same tiny protocol. Keeping transport detail
here means the router only reasons about health, cost, and fallback.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..config import Settings
from ..errors import PlatformError, RateLimited, Timeout, UpstreamError, UpstreamUnavailable

ROLE_SYSTEM = "system"
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"

CHARS_PER_TOKEN = 4


@dataclass(frozen=True, slots=True)
class Message:
    role: str
    content: str

    def to_dict(self) -> dict[str, str]:
        return {"role": self.role, "content": self.content}

    @staticmethod
    def user(content: str) -> Message:
        return Message(ROLE_USER, content)


@dataclass(frozen=True, slots=True)
class CompletionRequest:
    model: str
    messages: tuple[Message, ...]
    max_tokens: int | None = None
    temperature: float = 0.0
    stream: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def prompt_chars(self) -> int:
        return sum(len(message.content) for message in self.messages)

    def estimated_prompt_tokens(self) -> int:
        """Cheap, stable token estimate.

        Exact counting needs a tokenizer per model, which would put the hottest
        dependency on the budget gate; an estimate is enough to reject runaway
        requests before they are billed.
        """
        return self.prompt_chars // CHARS_PER_TOKEN + len(self.messages)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [message.to_dict() for message in self.messages],
            "temperature": self.temperature,
            "stream": self.stream,
        }
        if self.max_tokens is not None:
            payload["max_tokens"] = self.max_tokens
        payload.update(self.extra)
        return payload


@dataclass(frozen=True, slots=True)
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    def plus(self, other: Usage) -> Usage:
        return Usage(self.prompt_tokens + other.prompt_tokens, self.completion_tokens + other.completion_tokens)

    def to_dict(self) -> dict[str, int]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass(frozen=True, slots=True)
class Completion:
    text: str
    model: str
    provider: str
    usage: Usage
    finish_reason: str = "stop"

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "model": self.model,
            "provider": self.provider,
            "usage": self.usage.to_dict(),
            "finish_reason": self.finish_reason,
        }


@runtime_checkable
class Provider(Protocol):
    """One upstream model endpoint.

    ``healthy()`` stays separate from the circuit breaker on purpose: it covers
    cheap readiness checks, while the breaker learns only from real traffic.
    """

    name: str

    def complete(self, request: CompletionRequest) -> Completion: ...

    def stream(self, request: CompletionRequest) -> Iterator[str]: ...

    def healthy(self) -> bool: ...


def _classify(exc: BaseException, *, provider: str) -> PlatformError:
    """Translate transport failures into the shared error taxonomy."""
    if isinstance(exc, PlatformError):
        return exc
    if isinstance(exc, urllib.error.HTTPError):
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")[:500]
        except Exception:  # pragma: no cover - body may be unreadable
            body = ""
        details: dict[str, Any] = {"provider": provider, "status": exc.code, "body": body}
        if exc.code == 429:
            retry_after = float(exc.headers.get("Retry-After", 1) or 1) if exc.headers else 1.0
            return RateLimited("upstream rate limited us", details=details, retry_after_s=retry_after)
        if exc.code in (401, 403):
            return UpstreamError("upstream rejected our credentials", details=details)
        if exc.code == 404:
            return UpstreamError("unknown model on upstream", details=details)
        if exc.code >= 500:
            return UpstreamUnavailable("upstream is unhealthy", details=details)
        return UpstreamError("upstream call failed", details=details)
    if isinstance(exc, TimeoutError):
        return Timeout("upstream timed out", details={"provider": provider})
    if isinstance(exc, (urllib.error.URLError, ConnectionError, OSError)):
        return UpstreamUnavailable("upstream is unreachable", details={"provider": provider, "reason": str(exc)})
    return UpstreamError("upstream call failed", details={"provider": provider, "reason": str(exc)})


class _HttpProvider:
    """Shared JSON-over-HTTP plumbing so concrete providers stay tiny."""

    def __init__(self, *, name: str, base_url: str, timeout_s: float,
                 headers: dict[str, str] | None = None) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.headers = {"content-type": "application/json", **(headers or {})}

    def _request(self, path: str, payload: dict[str, Any] | None) -> bytes:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(f"{self.base_url}{path}", data=body, headers=self.headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                return response.read()
        except (urllib.error.URLError, ConnectionError, OSError, TimeoutError) as exc:
            raise _classify(exc, provider=self.name) from exc

    def _lines(self, path: str, payload: dict[str, Any]) -> Iterator[str]:
        """Yield decoded transport lines; each provider interprets its own framing."""
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers=self.headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                for raw in response:
                    text = raw.decode("utf-8", "replace").strip()
                    if text:
                        yield text
        except (urllib.error.URLError, ConnectionError, OSError, TimeoutError) as exc:
            raise _classify(exc, provider=self.name) from exc


class EchoProvider:
    """Deterministic in-process provider.

    It gives the gateway a real, dependency-free upstream so routing, fallback,
    and budget behaviour are testable offline, and local development works with
    no API key. Failures can be injected to exercise the breaker.
    """

    def __init__(self, *, name: str = "echo", fail_times: int = 0, chunk_size: int = 12,
                 failures: Sequence[type] | None = None) -> None:
        self.name = name
        self.fail_times = fail_times
        self.chunk_size = max(1, chunk_size)
        self._failures = list(failures or [])
        self.calls = 0
        self.is_healthy = True

    def _maybe_fail(self) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            if self._failures:
                raise self._failures[0]("injected echo failure")
            raise UpstreamUnavailable("injected echo failure", details={"provider": self.name})

    def complete(self, request: CompletionRequest) -> Completion:
        self._maybe_fail()
        prompt = request.messages[-1].content if request.messages else ""
        cap = request.max_tokens * CHARS_PER_TOKEN if request.max_tokens else None
        text = f"[{self.name}:{request.model}] {prompt}"[:cap]
        usage = Usage(
            prompt_tokens=request.estimated_prompt_tokens(),
            completion_tokens=len(text) // CHARS_PER_TOKEN,
        )
        return Completion(text=text, model=request.model, provider=self.name, usage=usage)

    def stream(self, request: CompletionRequest) -> Iterator[str]:
        completion = self.complete(request)
        for start in range(0, len(completion.text), self.chunk_size):
            yield completion.text[start:start + self.chunk_size]

    def healthy(self) -> bool:
        return self.is_healthy


class OpenAICompatibleProvider(_HttpProvider):
    """Speaks /chat/completions: OpenAI, Azure gateways, vLLM, LiteLLM, and friends."""

    def __init__(self, *, name: str = "openai", base_url: str, api_key: str = "", timeout_s: float = 60.0,
                 default_model: str = "gpt-4o-mini") -> None:
        headers = {"authorization": f"Bearer {api_key}"} if api_key else {}
        super().__init__(name=name, base_url=base_url, timeout_s=timeout_s, headers=headers)
        self.default_model = default_model

    def complete(self, request: CompletionRequest) -> Completion:
        raw = json.loads(self._request("/chat/completions", request.to_payload() | {"stream": False}))
        choice = (raw.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        usage_raw = raw.get("usage") or {}
        usage = Usage(int(usage_raw.get("prompt_tokens", 0)), int(usage_raw.get("completion_tokens", 0)))
        if not usage.total_tokens:
            usage = Usage(request.estimated_prompt_tokens(), len(message.get("content", "")) // CHARS_PER_TOKEN)
        return Completion(
            text=message.get("content", ""),
            model=raw.get("model", request.model),
            provider=self.name,
            usage=usage,
            finish_reason=choice.get("finish_reason", "stop"),
        )

    def stream(self, request: CompletionRequest) -> Iterator[str]:
        for line in self._lines("/chat/completions", request.to_payload() | {"stream": True}):
            if not line.startswith("data:"):
                continue
            data = line[len("data:"):].strip()
            if data == "[DONE]":
                return
            choices = (json.loads(data) or {}).get("choices") or []
            if not choices:
                continue
            delta = (choices[0].get("delta") or {}).get("content")
            if delta:
                yield delta

    def healthy(self) -> bool:
        return bool(self.base_url)


class OllamaProvider(_HttpProvider):
    """Local and edge models keep sensitive prompts on infrastructure you control."""

    def __init__(self, *, name: str = "ollama", base_url: str, timeout_s: float = 120.0) -> None:
        super().__init__(name=name, base_url=base_url, timeout_s=timeout_s)

    def _payload(self, request: CompletionRequest, *, stream: bool) -> dict[str, Any]:
        options: dict[str, Any] = {"temperature": request.temperature}
        if request.max_tokens:
            options["num_predict"] = request.max_tokens
        return {
            "model": request.model,
            "messages": [message.to_dict() for message in request.messages],
            "stream": stream,
            "options": options,
        }

    def complete(self, request: CompletionRequest) -> Completion:
        raw = json.loads(self._request("/api/chat", self._payload(request, stream=False)))
        usage = Usage(
            int(raw.get("prompt_eval_count", request.estimated_prompt_tokens())),
            int(raw.get("eval_count", 0)),
        )
        return Completion(
            text=(raw.get("message") or {}).get("content", ""),
            model=raw.get("model", request.model),
            provider=self.name,
            usage=usage,
        )

    def stream(self, request: CompletionRequest) -> Iterator[str]:
        for line in self._lines("/api/chat", self._payload(request, stream=True)):
            chunk = json.loads(line)
            piece = (chunk.get("message") or {}).get("content")
            if piece:
                yield piece
            if chunk.get("done"):
                return

    def healthy(self) -> bool:
        try:
            with urllib.request.urlopen(f"{self.base_url}/api/tags", timeout=min(3.0, self.timeout_s)):
                return True
        except Exception:
            return False


def build_providers(settings: Settings) -> list[Provider]:
    """Assemble the upstream pool from configuration.

    Echo is always present so a fresh deployment can be smoke tested without
    credentials; real providers join once their endpoints are configured.
    """
    providers: list[Provider] = [EchoProvider()]
    if settings.openai_api_key or "127.0.0.1" in settings.openai_base_url or "localhost" in settings.openai_base_url:
        providers.append(
            OpenAICompatibleProvider(
                base_url=settings.openai_base_url,
                api_key=settings.openai_api_key,
                timeout_s=settings.provider_timeout_s,
                default_model=settings.default_model,
            )
        )
    if settings.ollama_base_url:
        providers.append(OllamaProvider(base_url=settings.ollama_base_url, timeout_s=settings.provider_timeout_s))
    return providers


def messages_from_raw(raw: Sequence[dict[str, Any]]) -> tuple[Message, ...]:
    return tuple(
        Message(str(item.get("role", ROLE_USER)), str(item.get("content", "")))
        for item in raw
    )
