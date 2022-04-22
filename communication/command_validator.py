import copy

import jsonschema

MASTER_CMD_NAMES = ["connect", "disconnect", "finished", "task_done"]
WORKER_CMD_NAMES = ["map", "reduce", "terminate", "ping"]

CMD_SCHEMA = {
    "type": "object",
    "properties": {
        "host": {"type": "string"},
        "port": {"type": "integer"},
        "name": {"type": "string", "enum": []},
        "data": {"type": "object"},
    },
}


class CommandValidator:
    @staticmethod
    def get_full_schema(possible_commands: list[str]):
        schema = copy.deepcopy(CMD_SCHEMA)
        schema["properties"]["name"]["enum"] = possible_commands  # type: ignore
        return schema

    @staticmethod
    def validate(cmd: object, possible_commands: list[str]):
        try:
            jsonschema.validate(
                instance=cmd, schema=CommandValidator.get_full_schema(possible_commands)
            )
        except jsonschema.exceptions.ValidationError:
            return False
        return True

    @staticmethod
    def validate_master(cmd: object):
        return CommandValidator.validate(cmd, MASTER_CMD_NAMES)

    @staticmethod
    def validate_worker(cmd: object):
        return CommandValidator.validate(cmd, WORKER_CMD_NAMES)
