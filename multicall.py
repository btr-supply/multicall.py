from typing import Any, Final, Generator, List, Optional, Sequence, Tuple, final

from eth_typing import BlockIdentifier
from web3 import Web3
from asyncio import gather

from .call import Call
from ...utils import log_warn
from ...constants import (
    GAS_LIMIT,
    MULTICALL3_BYTECODE,
    MULTICALL3_ADDRESS,
    MULTICALL3_EXCEPTIONS,
)
from .utils import (
    chain_id,
    state_override_supported,
    get_async_w3,
)

@final
class NotSoBrightBatcher:
    """
    This class helps with processing a large volume of large multicalls.
    It's not so bright, but should quickly bring the batch size down to something reasonable for your node.
    """
    __slots__ = ("step",)

    def __init__(self) -> None:
        self.step = 10000

    def batch_calls(self, calls: List[Call], step: int) -> List[List[Call]]:
        """
        Batch calls into chunks of size `self.step`.
        """
        batches = []
        start = 0
        done = len(calls) - 1
        while True:
            end = start + step
            batches.append(calls[start:end])
            if end >= done:
                return batches
            start = end

    def split_calls(self, calls: List[Call]) -> Tuple[List[Call], List[Call]]:
        """
        Split calls into 2 batches in case request is too large.
        We do this to help us find optimal `self.step` value.
        """
        center = len(calls) // 2
        chunk_1 = calls[:center]
        chunk_2 = calls[center:]
        return chunk_1, chunk_2

    def rebatch(self, calls: List[Call]) -> Sequence[List[Call]]:
        # If a separate coroutine changed `step` after calls were last batched, we will use the new `step` for rebatching.
        if self.step <= len(calls) // 2:
            return self.batch_calls(calls, self.step)

        # Otherwise we will split calls in half.
        if self.step >= len(calls):
            new_step = round(len(calls) * 0.99) if len(calls) >= 100 else len(calls) - 1
            log_warn(
                f"Multicall batch size reduced from {self.step} to {new_step}. The failed batch had {len(calls)} calls."
            )
            self.step = new_step
        return self.split_calls(calls)


def _raise_or_proceed(e: Exception, ct_calls: int) -> None:
    """Depending on the exception, either raises or ignores and allows `batcher` to rebatch."""
    error_str = str(e).lower()
    rebatch_triggers = [
        # httpx errors
        "request entity too large",
        "payload too large",
        "time-out",
        "timeout",
        # generic connection errors
        "broken pipe",
        "connection reset by peer",
        # web3/node errors
        "out of gas",
        "out of memory",
        "server error", # generic server error
    ]
    if any(trigger in error_str for trigger in rebatch_triggers):
        if "out of gas" in error_str and ct_calls == 1:
            # A single call that is out of gas cannot be fixed by batching.
            raise e
        log_warn(f"Multicall batch of {ct_calls} calls failed with error: {e}. Re-batching...")
        return  # Proceed with re-batching
    raise e


class Multicall:
    __slots__ = (
        "calls",
        "block_id",
        "gas_limit",
        "w3",
        "chainid",
        "contract_address",
        "bytecode",
        "require_success",
        "batcher",
    )

    def __init__(
        self,
        calls: List[Call],
        w3: Optional[Web3] = None,
        block_id: Optional[BlockIdentifier] = None,
        gas_limit: int = GAS_LIMIT,
        require_success: bool = False,
    ):
        self.calls = calls
        self.block_id = block_id
        self.gas_limit = gas_limit
        self.require_success = require_success
        self.batcher = NotSoBrightBatcher()
        self.w3 = w3

        if w3 is None:
            from web3.auto import w3 as default_w3
            self.w3 = default_w3

        self.chainid: Final = chain_id(self.w3)
        self.w3 = get_async_w3(self.w3) # replace sync w3.eth with async w3.eth

        self.contract_address: Final = MULTICALL3_EXCEPTIONS.get(self.chainid, MULTICALL3_ADDRESS)
        self.bytecode: Final = MULTICALL3_BYTECODE

    def __await__(self) -> Generator[Any, Any, List[Any]]:
        return self.coroutine().__await__()

    async def coroutine(self) -> List[Any]:
        batches = self.batcher.batch_calls(self.calls, self.batcher.step)
        results_in_batches = await gather(*[self.fetch_outputs(batch) for batch in batches])
        return [item for batch_results in results_in_batches for item in batch_results]

    @property
    def aggregate(self) -> Call:
        """Create a Call object for the multicall contract."""
        multicall_sig = "tryAggregate(bool,(address,bytes)[])((bool,bytes)[])"

        if state_override_supported(self.w3):
            return Call(
                self.contract_address,
                multicall_sig,
                returns=None,
                _w3=self.w3,
                block_id=self.block_id,
                gas_limit=self.gas_limit,
                state_override_code=self.bytecode,
            )

        return Call(
            self.contract_address,
            multicall_sig,
            returns=None,
            _w3=self.w3,
            block_id=self.block_id,
            gas_limit=self.gas_limit,
        )

    def get_args(self, calls: List[Call]) -> List[Any]:
        """Prepare arguments for the multicall contract."""
        return [self.require_success, [[call.target, call.data] for call in calls]]

    async def fetch_outputs(self, calls: List[Call]) -> List[Any]:
        from ...utils import log_debug
        try:
            args = self.get_args(calls)
            log_debug(f"Multicall args for {len(calls)} calls: {len(args)} args")

            outputs = await self.aggregate.coroutine(args)
            log_debug(f"Multicall raw outputs: {len(outputs) if outputs else 0} results")

            # Process results
            results = []
            for i, (call, (success, output)) in enumerate(zip(calls, outputs)):
                try:
                    decoded_result = Call.decode_output(output, call.signature, call.returns, success)
                    log_debug(f"Call {i}: {call.function} -> {decoded_result}")
                    results.append(decoded_result)
                except Exception as e:
                    log_debug(f"Failed to decode call {i}: {e}")
                    results.append(None)

            return results
        except Exception as e:
            _raise_or_proceed(e, len(calls))

        # If we reach here, it means we need to re-batch and try again.
        sub_batches = self.batcher.rebatch(calls)
        results_in_batches = await gather(*[self.fetch_outputs(batch) for batch in sub_batches])
        return [item for batch_results in results_in_batches for item in batch_results]
