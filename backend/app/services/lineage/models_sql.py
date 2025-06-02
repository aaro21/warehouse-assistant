from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class ProcMetadata(SQLModel, table=True):
    __tablename__ = "proc_metadata"
    id: Optional[int] = Field(default=None, primary_key=True)
    proc_name: str
    proc_hash: str
    record_insert_datetime: datetime = Field(default_factory=datetime.utcnow)


class TableMap(SQLModel, table=True):
    __tablename__ = "table_map"
    id: Optional[int] = Field(default=None, primary_key=True)
    proc_id: int = Field(foreign_key="proc_metadata.id")
    dest_db: str
    dest_schema: str
    dest_table: str
    record_insert_datetime: datetime = Field(default_factory=datetime.utcnow)


class TableSource(SQLModel, table=True):
    __tablename__ = "table_source"
    id: Optional[int] = Field(default=None, primary_key=True)
    table_map_id: int = Field(foreign_key="table_map.id")
    src_db: str
    src_schema: str
    src_table: str
    role: str  # 'PRIMARY' or 'LOOKUP'
    join_predicate: Optional[str] = None
    record_insert_datetime: datetime = Field(default_factory=datetime.utcnow)


class ColumnMap(SQLModel, table=True):
    __tablename__ = "column_map"
    id: Optional[int] = Field(default=None, primary_key=True)
    table_source_id: int = Field(foreign_key="table_source.id")
    dest_column: str
    src_column: Optional[str] = None
    transform_expr: Optional[str] = None
    record_insert_datetime: datetime = Field(default_factory=datetime.utcnow)