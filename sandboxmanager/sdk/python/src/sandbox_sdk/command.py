from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

from ._models import COMPLETED, ERROR, RUNNING, TIMED_OUT, CommandBase  # noqa: F401  (re-exported)

if TYPE_CHECKING:
    from .client import Client


class Command(CommandBase):
    """One execution of a command in a sandbox.

    Returned by :meth:`Sandbox.exec` (with output already loaded) and by
    :meth:`Sandbox.commands` (output fetched on first access to ``stdout`` or
    ``stderr``, since the API's list endpoint omits it).

    Attributes: ``execution_id``, ``sandbox_id``, ``argv``, ``pod``, ``status``,
    ``exit_code``, ``started_at``, ``finished_at``; plus ``stdout``, ``stderr``,
    ``stdout_truncated``, ``stderr_truncated``, ``ok``, ``timed_out``, ``running``.
    """

    def __init__(self, data: Dict[str, Any], client: Client, *, output_loaded: bool = True):
        self._client = client
        super().__init__(data, output_loaded=output_loaded)

    def refresh(self) -> Command:
        """Re-read this execution (status and full output) from the API."""
        self._apply(self._client.get_execution(self.execution_id), output_loaded=True)
        return self

    def _ensure_output(self) -> None:
        if not self._output_loaded:
            self.refresh()
