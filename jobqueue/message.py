import uuid

from . import functions


class Message:

    def __init__(self, credentials: {str: any}, table_name: str, result: list) -> None:
        self._credentials: {str: any} = credentials
        self._table_name: str = table_name
        self._uuid: uuid.UUID = uuid.UUID(result[0])
        self._config: dict = result[2]
        self._groupname: str = result[3]
        self._priority: str = result[8]

    @property
    def uuid(self) -> uuid.UUID:
        return self._uuid

    @property
    def config(self) -> {str: any}:
        return self._config

    @property
    def groupname(self) -> str:
        return self._groupname

    @property
    def priority(self) -> str:
        return self._priority

    def mark_complete(self) -> None:
        functions.mark_job_as_done(self._credentials, self._table_name, self._uuid)

    def mark_failed(self) -> None:
        functions.mark_job_as_failed(self._credentials, self._table_name, self._uuid)
