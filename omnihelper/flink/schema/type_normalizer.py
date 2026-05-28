import re


class TypeNormalizer:
    TYPE_ALIASES = {
        "STRING": "VARCHAR",
        "INTEGER": "INT",
        "LONG": "BIGINT",
        "REAL": "FLOAT",
        "DOUBLE NOT NULL": "DOUBLE",
        "FLOAT NOT NULL": "FLOAT",
        "INT NOT NULL": "INT",
        "BIGINT NOT NULL": "BIGINT",
        "VARCHAR NOT NULL": "VARCHAR",
        "BOOLEAN NOT NULL": "BOOLEAN",
        "TIMESTAMP NOT NULL": "TIMESTAMP",
        "DATE NOT NULL": "DATE",
        "TINYINT": "TINYINT",
        "SMALLINT": "SMALLINT",
        "DECIMAL": "DECIMAL",
        "BINARY": "BINARY",
        "VARBINARY": "VARBINARY",
        "MULTISET": "MULTISET",
        "ROW": "ROW",
        "ARRAY": "ARRAY",
        "MAP": "MAP",
        "TIME": "TIME",
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE": "TIMESTAMP_LTZ",
        "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    }

    NUMERIC_PRIORITY = {
        "TINYINT": 1,
        "SMALLINT": 2,
        "INT": 3,
        "BIGINT": 4,
        "FLOAT": 5,
        "DOUBLE": 6,
        "DECIMAL": 7,
    }

    @staticmethod
    def normalize_type(type_str):
        if not type_str:
            return "unknown"

        type_str = type_str.strip()

        if type_str.upper().startswith("ROW"):
            return "ROW"

        if type_str.upper().startswith("ARRAY"):
            return "ARRAY"

        if type_str.upper().startswith("MAP"):
            return "MAP"

        if type_str.upper().startswith("MULTISET"):
            return "MULTISET"

        if type_str.upper().startswith("DECIMAL"):
            return "DECIMAL"

        if type_str.upper().startswith("VARCHAR"):
            return "VARCHAR"

        if type_str.upper().startswith("CHAR"):
            return "CHAR"

        if type_str.upper().startswith("TIMESTAMP_WITH_LOCAL_TIME_ZONE"):
            return "TIMESTAMP_LTZ"

        if type_str.upper().startswith("TIMESTAMP"):
            return "TIMESTAMP"

        upper = type_str.upper()
        return TypeNormalizer.TYPE_ALIASES.get(upper, upper)

    @staticmethod
    def normalize_type_for_match(type_str):
        normalized = TypeNormalizer.normalize_type(type_str)
        return normalized

    @staticmethod
    def parse_row_type(type_str):
        if not type_str:
            return []

        type_str = type_str.strip()
        if not type_str.upper().startswith("ROW"):
            return []

        inner = TypeNormalizer._extract_angle_brackets_content(type_str)
        if not inner:
            return []

        return TypeNormalizer._parse_row_fields(inner)

    @staticmethod
    def _extract_angle_brackets_content(type_str):
        start_idx = type_str.find("<")
        if start_idx == -1:
            return None

        depth = 0
        for i in range(start_idx, len(type_str)):
            if type_str[i] == "<":
                depth += 1
            elif type_str[i] == ">":
                depth -= 1
                if depth == 0:
                    return type_str[start_idx + 1 : i]

        return None

    @staticmethod
    def _parse_row_fields(fields_str):
        fields = []
        parts = TypeNormalizer._split_row_fields(fields_str)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            match = re.match(r"^(\w+)\s+(.+)$", part.strip())
            if not match:
                continue

            field_name = match.group(1)
            field_type_str = match.group(2).strip()

            normalized_type = TypeNormalizer.normalize_type(field_type_str)
            nested_fields = TypeNormalizer.parse_row_type(field_type_str)

            fields.append({
                "field_name": field_name,
                "field_type": normalized_type,
                "original_type": field_type_str,
                "nested_fields": nested_fields,
            })

        return fields

    @staticmethod
    def _split_row_fields(fields_str):
        parts = []
        current = []
        depth = 0

        for char in fields_str:
            if char == "<":
                depth += 1
                current.append(char)
            elif char == ">":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(char)

        if current:
            parts.append("".join(current))

        return parts

    @staticmethod
    def find_common_type(type1, type2):
        if not type1 or not type2:
            return None

        if type1 == type2:
            return type1

        if type1 == "unknown" or type2 == "unknown":
            return "unknown"

        t1 = TypeNormalizer.normalize_type(type1)
        t2 = TypeNormalizer.normalize_type(type2)

        if t1 == t2:
            return t1

        if t1 in TypeNormalizer.NUMERIC_PRIORITY and t2 in TypeNormalizer.NUMERIC_PRIORITY:
            p1 = TypeNormalizer.NUMERIC_PRIORITY[t1]
            p2 = TypeNormalizer.NUMERIC_PRIORITY[t2]
            return t1 if p1 > p2 else t2

        if t1 == "VARCHAR" or t2 == "VARCHAR":
            return "VARCHAR"

        if t1 == "CHAR" or t2 == "CHAR":
            return "VARCHAR"

        datetime_priority = {"DATE": 1, "TIMESTAMP": 2, "TIMESTAMP_LTZ": 3}
        if t1 in datetime_priority and t2 in datetime_priority:
            p1 = datetime_priority[t1]
            p2 = datetime_priority[t2]
            return t1 if p1 > p2 else t2

        if t1 == "BOOLEAN" or t2 == "BOOLEAN":
            return "VARCHAR"

        return None

    @staticmethod
    def find_common_type_multi(types):
        if not types:
            return None

        result = types[0]
        for t in types[1:]:
            result = TypeNormalizer.find_common_type(result, t)
            if result is None:
                return None

        return result

    @staticmethod
    def expand_row_type(type_str):
        """
        将 ROW 类型递归展开为嵌套字段的类型列表。
        例如: ROW<id INT, name VARCHAR, addr ROW<city VARCHAR, zip INT>>
        返回: ["INT", "VARCHAR", "VARCHAR", "INT"]
        对于非 ROW 类型，返回包含标准化类型的列表。
        """
        if not type_str:
            return ["unknown"]
        type_str = type_str.strip()
        if not type_str.upper().startswith("ROW"):
            return [TypeNormalizer.normalize_type(type_str)]
        inner = TypeNormalizer._extract_angle_brackets_content(type_str)
        if not inner:
            return ["ROW"]
        parts = TypeNormalizer._split_row_fields(inner)
        expanded = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            match = re.match(r"^(\w+)\s+(.+)$", part.strip())
            if not match:
                continue
            field_type_str = match.group(2).strip()
            # 递归展开嵌套的 ROW 类型
            if field_type_str.upper().startswith("ROW"):
                nested_expanded = TypeNormalizer.expand_row_type(field_type_str)
                expanded.extend(nested_expanded)
            else:
                expanded.append(TypeNormalizer.normalize_type(field_type_str))
        return expanded if expanded else ["ROW"]
