import csv
import os
from collections import defaultdict

from omnihelper.constants.flink_constants import CsvColumns
from omnihelper.flink.schema.type_normalizer import TypeNormalizer
from omnihelper.util.log import logger


class TableSchemaReader:
    @staticmethod
    def read_table_schema(csv_path):
        table_schema = defaultdict(list)
        if not csv_path or not os.path.exists(csv_path):
            logger.warning(f"Table schema CSV file not found: {csv_path}")
            return table_schema

        try:
            rows = TableSchemaReader._read_csv_rows(csv_path)
            for table_name, field_name, field_type in rows:
                if not table_name or not field_name or not field_type:
                    continue

                normalized_type = TypeNormalizer.normalize_type(field_type)
                nested_fields = TypeNormalizer.parse_row_type(field_type)

                table_schema[table_name].append({
                    "field_name": field_name,
                    "field_type": normalized_type,
                    "original_type": field_type,
                    "nested_fields": nested_fields,
                })

            logger.info(f"Loaded table schema from {csv_path}, total {len(table_schema)} tables")
        except Exception as e:
            logger.warning(f"Failed to read table schema CSV: {e}")

        return table_schema

    @staticmethod
    def _read_csv_rows(csv_path):
        rows = []
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            required = set(CsvColumns.get_required_columns())
            if not required.issubset(set(reader.fieldnames or [])):
                logger.warning(
                    f"CSV file missing required columns. Required: {required}, "
                    f"Found: {set(reader.fieldnames or [])}"
                )
                return rows

            for row in reader:
                table_name = row.get(CsvColumns.TABLE_NAME, "").strip()
                field_name = row.get(CsvColumns.FIELD_NAME, "").strip()
                field_type = row.get(CsvColumns.FIELD_TYPE, "").strip()
                if not table_name or not field_name or not field_type:
                    continue

                field_type = TableSchemaReader._reconstruct_type_from_row(field_type, row)
                rows.append((table_name, field_name, field_type))

        return rows

    @staticmethod
    def _reconstruct_type_from_row(field_type, row):
        if not field_type:
            return field_type

        open_angle = field_type.count("<")
        close_angle = field_type.count(">")
        if open_angle <= close_angle:
            return field_type

        extra_parts = []
        for key in sorted(row.keys()):
            if key.startswith(CsvColumns.FIELD_TYPE) and key != CsvColumns.FIELD_TYPE:
                val = row.get(key, "").strip()
                if val:
                    extra_parts.append(val)

        if extra_parts:
            field_type = field_type + "," + ",".join(extra_parts)

        if field_type.count("<") > field_type.count(">"):
            field_type = field_type + ">"

        return field_type

    @staticmethod
    def build_column_type_mapping(table_schema, tables_used=None):
        column_type = {}
        table_column_type = {}

        tables_to_process = (
            {t: table_schema[t] for t in tables_used if t in table_schema}
            if tables_used
            else table_schema
        )

        for table_name, columns in tables_to_process.items():
            for col_info in columns:
                field_name = col_info["field_name"]
                field_type = col_info["field_type"]

                key = f"{table_name}.{field_name}".lower()
                table_column_type[key] = field_type

                name_lower = field_name.lower()
                if name_lower not in column_type:
                    column_type[name_lower] = field_type
                else:
                    logger.debug(
                        f"Column name conflict: '{field_name}' exists in multiple tables. "
                        f"Use table_name.field_name format for disambiguation."
                    )

        return column_type, table_column_type

    @staticmethod
    def resolve_nested_field_type(column_info, field_path):
        if not field_path or not column_info.get("nested_fields"):
            return column_info.get("field_type", "unknown")

        nested = column_info["nested_fields"]
        for part in field_path:
            found = False
            for field in nested:
                if field["field_name"] == part:
                    nested = field.get("nested_fields", [])
                    result_type = field["field_type"]
                    found = True
                    break
            if not found:
                return "unknown"

        return result_type
