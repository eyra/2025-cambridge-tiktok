from collections.abc import Generator
from port.script import process
from port.api.commands import CommandSystemExit
from port.api.file_utils import AsyncFileAdapter


class ScriptWrapper(Generator):
    def __init__(self, script):
        self.script = script
        self.started = False

    def send(self, data):
        # Automatically wrap JS file readers with AsyncFileAdapter
        if data and getattr(data, '__type__', None) == "PayloadFile":
            data.value = AsyncFileAdapter(data.value)

        try:
            # Start the generator on first call
            if not self.started:
                self.started = True
                command = self.script.send(None)
                # If data was provided, send it now
                if data is not None:
                    command = self.script.send(data)
            else:
                command = self.script.send(data)
        except StopIteration:
            return CommandSystemExit(0, "End of script").toDict()
        else:
            return command.toDict()

    def throw(self, type=None, value=None, traceback=None):
        raise StopIteration


def start(data):
    script = process(data)
    return ScriptWrapper(script)
