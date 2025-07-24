from web3 import AsyncHTTPProvider, AsyncWeb3, Web3, WebSocketProvider
from web3.providers.async_base import AsyncBaseProvider

from ... import state
from ...utils.decorators import cache
from ...constants import NO_STATE_OVERRIDE


@cache(ttl=-1, maxsize=64)
def chain_id(w3: Web3) -> int:
    return w3.eth.chain_id

def get_endpoint(w3: Web3) -> str:
    provider = w3.provider
    if isinstance(provider, str):
        return provider
    if hasattr(provider, "_active_provider"):
        provider = provider._get_active_provider(False)
    return provider.endpoint_uri  # type: ignore [no-any-return]


@cache(maxsize=None)
def get_async_w3(w3: Web3) -> Web3:
    if w3.eth.is_async and isinstance(w3.provider, AsyncBaseProvider):
      w3.provider._request_kwargs["timeout"] = float(state.args.ingestion_timeout)
      return w3

    endpoint = get_endpoint(w3)
    provider_cls = WebSocketProvider if endpoint.startswith(("wss:", "ws:")) else AsyncHTTPProvider
    return AsyncWeb3(provider=provider_cls(endpoint, {"timeout": float(state.args.ingestion_timeout)}), middleware=[])


@cache(maxsize=None)
def state_override_supported(w3: Web3) -> bool:
    return chain_id(w3) not in NO_STATE_OVERRIDE
