# mypy: disable-error-code="attr-defined"
from typing import Any, Callable, Final, Generator, Iterable, List, Optional, Tuple, Union
from eth_typing import Address, ChecksumAddress, HexAddress, HexStr
from eth_typing.abi import Decodable
from web3 import Web3

from ...constants import ASYNC_SEMAPHORE
from .signature import Signature, get_signature
from .utils import (
    chain_id,
    get_async_w3,
    state_override_supported,
)

AnyAddress = Union[str, Address, ChecksumAddress, HexAddress]


class Call:
    __slots__ = (
        "target",
        "returns",
        "block_id",
        "gas_limit",
        "state_override_code",
        "w3",
        "origin",
        "function",
        "args",
        "signature",
    )

    def __init__(
        self,
        target: AnyAddress,
        function: Union[str, List[Union[str, Any]]],
        returns: Optional[Iterable[Tuple[str, Callable]]] = None,
        block_id: Optional[int] = None,
        gas_limit: Optional[int] = None,
        state_override_code: Optional[HexStr] = None,
        _w3: Optional[Web3] = None,
        origin: Optional[AnyAddress] = None,
    ) -> None:
        self.target: Final = Web3.to_checksum_address(target)
        self.returns: Final = returns
        self.block_id: Final = block_id
        self.gas_limit: Final = gas_limit
        self.state_override_code: Final = state_override_code
        self.w3: Final = _w3
        self.origin: Final = Web3.to_checksum_address(origin) if origin else None

        self.function: Final = function[0] if isinstance(function, list) else function
        self.args: Final = function[1:] if isinstance(function, list) else None
        self.signature: Final = get_signature(self.function)

    def __repr__(self) -> str:
        string = f"<Call {self.function} on {self.target[:8]}"
        if self.block_id is not None:
            string += f" block={self.block_id}"
        if self.returns is not None:
            string += f" returns={self.returns}"
        return f"{string}>"

    @property
    def data(self) -> bytes:
        return self.signature.encode_data(self.args)

    @staticmethod
    def decode_output(
        output: Decodable,
        signature: Signature,
        returns: Optional[Iterable[Tuple[str, Optional[Callable]]]] = None,
        success: Optional[bool] = None,
    ) -> Any:

        if success is None:
            apply_handler = lambda handler, value: handler(value)
        else:
            apply_handler = lambda handler, value: handler(success, value)

        if success is None or success:
            try:
                decoded = signature.decode_data(output)
            except:
                success, decoded = False, [None] * (len(returns) if returns else 1)
        else:
            decoded = [None] * (len(returns) if returns else 1)

        if returns:
            return {
                name: apply_handler(handler, value) if handler else value
                for (name, handler), value in zip(returns, decoded)
            }
        else:
            return decoded if len(decoded) > 1 else decoded[0]

    def __call__(
        self,
        args: Optional[Any] = None,
        _w3: Optional[Web3] = None,
        *,
        block_id: Optional[int] = None,
    ) -> Any:
        w3 = self.w3 or _w3
        if w3 is None:
            from web3.auto import w3 as default_w3
            w3 = default_w3

        call_args = prep_args(
            self.target,
            self.signature,
            args or self.args,
            block_id or self.block_id,
            self.origin,
            self.gas_limit,
            self.state_override_code,
        )
        output = w3.eth.call(*call_args)
        return Call.decode_output(
            output,
            self.signature,
            self.returns,
        )

    def __await__(self) -> Generator[Any, Any, Any]:
        return self.coroutine().__await__()

    async def coroutine(
        self,
        args: Optional[Any] = None,
        _w3: Optional[Web3] = None,
        *,
        block_id: Optional[int] = None,
    ) -> Any:
        w3 = self.w3 or _w3
        if w3 is None:
            from web3.auto import w3 as default_w3
            w3 = default_w3

        if self.state_override_code and not state_override_supported(w3):
            raise ValueError(f"State override is not supported on chain {chain_id(w3)}.")

        async with ASYNC_SEMAPHORE:
            output = await get_async_w3(w3).eth.call(
                *prep_args(
                    self.target,
                    self.signature,
                    args or self.args,
                    block_id or self.block_id,
                    self.origin,
                    self.gas_limit,
                    self.state_override_code,
                )
            )

        return Call.decode_output(output, self.signature, self.returns)


def prep_args(
    target: str,
    signature: Signature,
    args: Optional[Any],
    block_id: Optional[int],
    origin: Optional[ChecksumAddress],
    gas_limit: Optional[int],
    state_override_code: Optional[HexStr],
) -> List[Any]:

    calldata = signature.encode_data(args)

    call_dict = {"to": target, "data": calldata}
    prepared_args: List[Any] = [call_dict]

    if block_id is not None:
        prepared_args.append(block_id)

    if origin:
        call_dict["from"] = origin

    if gas_limit:
        call_dict["gas"] = gas_limit

    if state_override_code:
        if block_id is None:
            # Add a default block identifier if none is present, as it's required for state override
            prepared_args.append('latest')
        prepared_args.append({target: {"code": state_override_code}})

    return prepared_args
