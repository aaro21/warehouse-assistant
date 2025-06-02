

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class ColumnMapping(BaseModel):
    dest_column: str
    src_column: Optional[str] = None
    transform_expr: Optional[str] = None


class TableSource(BaseModel):
    src_db: str
    src_schema: str
    src_table: str
    role: str  # 'PRIMARY' or 'LOOKUP'
    join_predicate: Optional[str] = None
    columns: List[ColumnMapping]


class TableMap(BaseModel):
    dest_db: str
    dest_schema: str
    dest_table: str
    sources: List[TableSource]


class ProcMetadata(BaseModel):
    proc_name: str
    proc_hash: str
    record_insert_datetime: Optional[datetime] = None
    table_map: TableMap