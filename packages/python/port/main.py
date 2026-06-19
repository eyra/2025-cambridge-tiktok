import logging
from collections import deque
from collections.abc import Generator
from port.script import process
from port.api.commands import CommandSystemExit, FlushLogs
from port.api.file_utils import AsyncFileAdapter
from port.api.logging import LogForwardingHandler


class ScriptWrapper(Generator):
    def __init__(self, script):
        self.script = script
        self.queue = deque()

    def add_log_handler(self, logger_name="port.script"):
        """Attach a handler to the named logger that forwards log records as CommandSystemLog commands."""
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(LogForwardingHandler(self.queue))

    def send(self, data):
        while True:
            if self.queue:
                return self.queue.popleft()

            if data and getattr(data, '__type__') == "PayloadFile":
                data.value = AsyncFileAdapter(data.value)
            try:
                command = self.script.send(data)
            except StopIteration:
                return self.flush_logs() or CommandSystemExit(0, "End of script").toDict()

            if command is FlushLogs:
                data = None
                continue

            self.queue.append(command.toDict())
            data = None

    def flush_logs(self):
        while self.queue:
            item = self.queue.popleft()
            if item.get("__type__") == "CommandSystemLog":
                return item
        return None

    def throw(self, type=None, value=None, traceback=None):
        raise StopIteration


# CAMBRIDGE-FORK: This file diverges from upstream Feldspar — `data` is a
# {sessionId, locale} dict rather than a bare sessionId int. See the matching
# divergences in py_worker.js, assembly.ts, worker_engine.ts,
# script_host_component.tsx, and port/script.py `process(data)`. When syncing
# feldspar/develop, keep this signature.
def start(data):
    script = process(data)
    wrapper = ScriptWrapper(script)
    wrapper.add_log_handler()
    return wrapper
