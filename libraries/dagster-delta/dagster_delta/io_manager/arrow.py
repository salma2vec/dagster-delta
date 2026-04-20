from collections.abc import Sequence
from typing import Union

import dagster as dg
from arro3.core import RecordBatchReader, Table
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable
from dagster._core.storage.db_io_manager import DbTypeHandler
from deltalake import DeltaTable

from dagster_delta._handler.base import ArrowTypes, DeltalakeBaseArrowTypeHandler
from dagster_delta.io_manager.base import (
    BaseDeltaLakeIOManager as BaseDeltaLakeIOManager,
    TableConnection,
)


class DeltaLakePyarrowIOManager(BaseDeltaLakeIOManager):  # noqa: D101
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:  # noqa: D102
        return [_DeltaLakePyArrowTypeHandler()]


class _DeltaLakePyArrowTypeHandler(DeltalakeBaseArrowTypeHandler[ArrowTypes]):  # noqa: D101
    def load_input(self, context, table_slice, connection: TableConnection) -> ArrowTypes:  # noqa: ANN001
        if context.dagster_type.typing_type == DeltaTable:
            version = (context.definition_metadata or {}).get("table_version")
            return DeltaTable(
                table_uri=connection.table_uri,
                storage_options=connection.storage_options,
                version=version,
            )

        return super().load_input(context, table_slice, connection)

    def from_arrow(
        self,
        obj: Union[ArrowStreamExportable, ArrowArrayExportable],
        target_type: type[ArrowTypes],  # type: ignore
    ) -> ArrowTypes:  # noqa: D102 # type: ignore
        data = RecordBatchReader.from_arrow(obj)

        if target_type == Table:
            return data.read_all()

        try:
            import pyarrow as pa

            if target_type == pa.Table:
                return pa.table(data)
            elif target_type == pa.RecordBatchReader:
                return pa.RecordBatchReader.from_stream(data)
        except ImportError:
            pass
        return data

    def to_arrow(
        self,
        obj: Union[ArrowStreamExportable, ArrowArrayExportable],
    ) -> ArrowStreamExportable:  # noqa: D102
        if isinstance(obj, DeltaTable):
            raise TypeError("DeltaTable is supported as an input type, not as an output value")

        return RecordBatchReader.from_arrow(obj)

    def get_output_stats(self, obj: ArrowTypes) -> dict[str, dg.MetadataValue]:  # noqa: ARG002 # type: ignore
        """Returns output stats to be attached to the the context.

        Args:
            obj (ArrowTypes): Union[pa.Table, pa.RecordBatchReader, ds.Dataset]

        Returns:
            Mapping[str, MetadataValue]: metadata stats
        """
        return {}

    @property
    def supported_types(self) -> Sequence[type[object]]:
        """Returns the supported dtypes for this typeHandler"""
        supported_types = [Table, RecordBatchReader, DeltaTable]
        try:
            import pyarrow as pa

            supported_types.extend([pa.Table, pa.RecordBatchReader])
        except ImportError:
            pass

        return supported_types
