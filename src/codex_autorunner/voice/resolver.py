from __future__ import annotations

import logging
import os
from typing import Mapping, Optional

from ..core.optional_dependencies import missing_optional_dependencies
from .config import VoiceConfig
from .provider import SpeechProvider
from .provider_catalog import (
    local_voice_provider_spec,
    missing_local_voice_runtime_commands,
    normalize_voice_provider,
)
from .providers import (
    LocalWhisperProvider,
    MlxWhisperProvider,
    OpenAIWhisperProvider,
    build_local_whisper_provider,
    build_mlx_whisper_provider,
    build_speech_provider,
)


def resolve_speech_provider(
    voice_config: VoiceConfig,
    logger: Optional[logging.Logger] = None,
    env: Optional[Mapping[str, str]] = None,
) -> SpeechProvider:
    """
    Resolve the configured speech provider. Raises when disabled or unknown.
    """
    if not voice_config.enabled:
        raise ValueError("Voice features are disabled in config")

    provider_name = normalize_voice_provider(voice_config.provider)
    provider_configs = voice_config.providers or {}
    runtime_env = env if env is not None else os.environ
    if not provider_name:
        raise ValueError("No voice provider configured")

    if provider_name == OpenAIWhisperProvider.name:
        provider_cfg = provider_configs.get(provider_name, {})
        api_key_env = "OPENAI_API_KEY"
        if isinstance(provider_cfg, Mapping):
            raw_api_key_env = provider_cfg.get("api_key_env")
            if isinstance(raw_api_key_env, str) and raw_api_key_env.strip():
                api_key_env = raw_api_key_env.strip()
        api_key = str(runtime_env.get(api_key_env, "")).strip().strip('"').strip(
            "'"
        ).strip("`").strip()
        if not api_key:
            fallback_provider = _resolve_local_fallback_provider(
                provider_configs,
                logger=logger,
            )
            if fallback_provider is not None:
                if logger is not None:
                    logger.info(
                        "Voice provider fallback selected: configured %s but %s is unset; using %s instead.",
                        provider_name,
                        api_key_env,
                        fallback_provider.name,
                    )
                return fallback_provider
            raise ValueError(
                f"OpenAI Whisper provider requires API key env '{api_key_env}' to be set. "
                "No local voice provider is currently usable; install a local voice "
                "provider or configure the API key."
            )
        return build_speech_provider(
            provider_configs.get(provider_name, {}),
            warn_on_remote_api=voice_config.warn_on_remote_api,
            env=runtime_env,
            logger=logger,
        )
    if provider_name == LocalWhisperProvider.name:
        provider_cfg = provider_configs.get(provider_name)
        if not isinstance(provider_cfg, Mapping):
            provider_cfg = provider_configs.get(LocalWhisperProvider.name, {})
        return build_local_whisper_provider(
            provider_cfg if isinstance(provider_cfg, Mapping) else {},
            logger=logger,
        )
    if provider_name == MlxWhisperProvider.name:
        provider_cfg = provider_configs.get(provider_name)
        if not isinstance(provider_cfg, Mapping):
            provider_cfg = provider_configs.get(MlxWhisperProvider.name, {})
        return build_mlx_whisper_provider(
            provider_cfg if isinstance(provider_cfg, Mapping) else {},
            logger=logger,
        )

    raise ValueError(f"Unsupported voice provider '{provider_name}'")


def _resolve_local_fallback_provider(
    provider_configs: Mapping[str, object],
    *,
    logger: Optional[logging.Logger],
) -> Optional[SpeechProvider]:
    for provider_name in (LocalWhisperProvider.name, MlxWhisperProvider.name):
        provider = _build_local_provider_if_usable(
            provider_name,
            provider_configs,
            logger=logger,
        )
        if provider is not None:
            return provider
    return None


def _build_local_provider_if_usable(
    provider_name: str,
    provider_configs: Mapping[str, object],
    *,
    logger: Optional[logging.Logger],
) -> Optional[SpeechProvider]:
    provider_spec = local_voice_provider_spec(provider_name)
    if provider_spec is None:
        return None
    normalized_name, deps, _extra = provider_spec
    if missing_optional_dependencies(deps):
        return None
    if missing_local_voice_runtime_commands(normalized_name):
        return None

    provider_cfg = provider_configs.get(normalized_name, {})
    if not isinstance(provider_cfg, Mapping):
        provider_cfg = {}
    if normalized_name == LocalWhisperProvider.name:
        return build_local_whisper_provider(provider_cfg, logger=logger)
    if normalized_name == MlxWhisperProvider.name:
        return build_mlx_whisper_provider(provider_cfg, logger=logger)
    return None
