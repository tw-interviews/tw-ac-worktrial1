from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

from ._models import CommandBase
from .errors import OutputNotLoadedError

if TYPE_CHECKING:
    from .async_client import AsyncClient


class AsyncCommand(CommandBase):
    """One execution of a command in a sandbox; the async twin of :class:`Command`.

    Commands returned by :meth:`AsyncSandbox.exec` and
    :meth:`AsyncSandbox.get_command` have their output loaded. Commands from
    :meth:`AsyncSandbox.commands` don't (the list endpoint omits it), and since
    a property can't be awaited, reading ``stdout``/``stderr`` on them raises
    :class:`OutputNotLoadedError` until you ``await cmd.refresh()``, or list
    with ``commands(with_output=True)``.
    """

    def __init__(self, data: Dict[str, Any], client: AsyncClient, *, output_loaded: bool = True):
        self._client = client
        super().__init__(data, output_loaded=output_loaded)

    async def refresh(self) -> AsyncCommand:
        """Re-read this execution (status and full output) from the API."""
        self._apply(await self._client.get_execution(self.execution_id), output_loaded=True)
        return self

    def _ensure_output(self) -> None:
        if not self._output_loaded:
            raise OutputNotLoadedError(
                f"output for execution {self.execution_id} isn't loaded; "
                "call `await cmd.refresh()` first, or use `commands(with_output=True)`"
            )
