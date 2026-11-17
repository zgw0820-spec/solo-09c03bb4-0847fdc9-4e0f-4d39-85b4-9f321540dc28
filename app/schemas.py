"""Request/response schemas surfaced in the OpenAPI (Swagger) document."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

# Re-declared as plain literals so the OpenAPI document stays self-contained
# without importing the pool implementation into the schema layer.
PriorityLiteral = Literal["normal", "urgent"]


class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句（允许 WITH/VALUES/TABLE，"
            "禁止多语句、写操作、SELECT INTO 等）"
        ),
        examples=["SELECT id, device_id, severity FROM fault_logs LIMIT 5"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "总截止时间（毫秒），从请求进入起计时，包含排队等连接的时间"
        ),
    )
    priority: PriorityLiteral = Field(
        default="normal",
        description=(
            "紧急等级（可选，默认 normal 普通）。urgent 紧急请求可在等待队列"
            "中越过先入队的普通请求；只调整等待顺序，不中断正在执行的 SQL。"
            "只要有普通请求在等，连续放行最多两条紧急请求后必须放行一条普通"
            "请求。等待期间到期或被撤销的请求不占用名额、不发送 SQL。"
        ),
    )


class QueryResponse(BaseModel):
    status: Literal["ok", "timeout"]
    executed: bool = Field(
        description="语句是否真正发送到了数据库后端"
    )
    priority: PriorityLiteral = Field(
        description="本次请求实际采用的紧急等级"
    )
    wait_ms: float | None = None
    execution_ms: float | None = None
    row_count: int | None = None
    rows: list[list[Any]] | None = None
    cancelled: bool | None = None
    connection_discarded: bool | None = None
    late_result_ignored: bool | None = None
    detail: str | None = None


class ErrorResponse(BaseModel):
    error: dict[str, str]


class HealthResponse(BaseModel):
    status: Literal["healthy"]
    pool: dict[str, Any]
