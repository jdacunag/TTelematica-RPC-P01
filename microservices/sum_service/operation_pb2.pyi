from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SumRequest(_message.Message):
    __slots__ = ("a", "b", "operation_id")
    A_FIELD_NUMBER: _ClassVar[int]
    B_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    a: int
    b: int
    operation_id: str
    def __init__(self, a: _Optional[int] = ..., b: _Optional[int] = ..., operation_id: _Optional[str] = ...) -> None: ...

class SumResponse(_message.Message):
    __slots__ = ("result", "success", "error_message", "operation_id")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    result: int
    success: bool
    error_message: str
    operation_id: str
    def __init__(self, result: _Optional[int] = ..., success: bool = ..., error_message: _Optional[str] = ..., operation_id: _Optional[str] = ...) -> None: ...

class StatusRequest(_message.Message):
    __slots__ = ("service_id",)
    SERVICE_ID_FIELD_NUMBER: _ClassVar[int]
    service_id: str
    def __init__(self, service_id: _Optional[str] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("status", "message", "uptime")
    class ServiceStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[StatusResponse.ServiceStatus]
        RUNNING: _ClassVar[StatusResponse.ServiceStatus]
        DEGRADED: _ClassVar[StatusResponse.ServiceStatus]
        DOWN: _ClassVar[StatusResponse.ServiceStatus]
    UNKNOWN: StatusResponse.ServiceStatus
    RUNNING: StatusResponse.ServiceStatus
    DEGRADED: StatusResponse.ServiceStatus
    DOWN: StatusResponse.ServiceStatus
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    UPTIME_FIELD_NUMBER: _ClassVar[int]
    status: StatusResponse.ServiceStatus
    message: str
    uptime: int
    def __init__(self, status: _Optional[_Union[StatusResponse.ServiceStatus, str]] = ..., message: _Optional[str] = ..., uptime: _Optional[int] = ...) -> None: ...

class AsyncOperationRequest(_message.Message):
    __slots__ = ("operation_id",)
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    def __init__(self, operation_id: _Optional[str] = ...) -> None: ...

class AsyncOperationResponse(_message.Message):
    __slots__ = ("status", "result", "message")
    class OperationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[AsyncOperationResponse.OperationStatus]
        PENDING: _ClassVar[AsyncOperationResponse.OperationStatus]
        PROCESSING: _ClassVar[AsyncOperationResponse.OperationStatus]
        COMPLETED: _ClassVar[AsyncOperationResponse.OperationStatus]
        FAILED: _ClassVar[AsyncOperationResponse.OperationStatus]
        CANCELLED: _ClassVar[AsyncOperationResponse.OperationStatus]
    UNKNOWN: AsyncOperationResponse.OperationStatus
    PENDING: AsyncOperationResponse.OperationStatus
    PROCESSING: AsyncOperationResponse.OperationStatus
    COMPLETED: AsyncOperationResponse.OperationStatus
    FAILED: AsyncOperationResponse.OperationStatus
    CANCELLED: AsyncOperationResponse.OperationStatus
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: AsyncOperationResponse.OperationStatus
    result: SumResponse
    message: str
    def __init__(self, status: _Optional[_Union[AsyncOperationResponse.OperationStatus, str]] = ..., result: _Optional[_Union[SumResponse, _Mapping]] = ..., message: _Optional[str] = ...) -> None: ...
