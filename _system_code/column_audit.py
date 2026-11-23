"""Profile populated fields before relational projection; never infer emptiness from a schema."""


def column_plan(columns, counts, row_count, required=(), protected=()):
    if row_count == 0:
        raise ValueError('The loaded dataset is empty; column emptiness cannot be established.')
    available = set(columns)
    required = set(required)
    protected = set(protected)
    empty = sorted(c for c in columns if counts[c] == 0)
    return {
        'missing_required': sorted(required - available),
        'empty_required': sorted(required.intersection(empty)),
        'empty_protected': sorted(protected.intersection(empty)),
        'drop_candidates': sorted(set(empty) - required - protected),
    }


def reviewed_drop_columns(plan, approved=()):
    """Keep every column by default; accept only explicitly reviewed candidates."""
    invalid = set(approved) - set(plan['drop_candidates'])
    if invalid:
        raise ValueError(f'Not optional-empty drop candidates (may be protected): {sorted(invalid)}')
    return sorted(set(approved))


def profile_columns(frame):
    """One distributed aggregate; collect only one count per top-level column.

    Empty means null, blank string, floating NaN, or zero-length array/map.
    A string containing '[]', '{}' or 'null' is retained for explicit semantic review.
    Zero, False, and structs with fields are not automatically classified as empty.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, ArrayType, MapType, FloatType, DoubleType
    expressions = [F.count(F.lit(1)).alias('__rows')]
    for i, field in enumerate(frame.schema.fields):
        value = F.col('`' + field.name.replace('`', '``') + '`')
        present = value.isNotNull()
        if isinstance(field.dataType, StringType):
            present = present & (F.length(F.trim(value)) > 0)
        elif isinstance(field.dataType, (ArrayType, MapType)):
            present = present & (F.size(value) > 0)
        elif isinstance(field.dataType, (FloatType, DoubleType)):
            present = present & ~F.isnan(value)
        expressions.append(F.sum(F.when(present, 1).otherwise(0)).alias(f'c{i}'))
    totals = frame.agg(*expressions).first().asDict()
    return totals['__rows'], {field.name: int(totals[f'c{i}'] or 0) for i, field in enumerate(frame.schema.fields)}


def add_dimension_key(frame, key, attributes):
    """Reuse supplied keys. Derive missing keys once before projecting related tables."""
    from pyspark.sql import functions as F
    if key in frame.columns:
        return frame
    payload = (F.to_json(F.struct(*[F.col(c) for c in attributes]), {'ignoreNullFields': 'false'})
               if attributes else F.lit('unknown_dimension'))
    return frame.withColumn(key, F.sha2(payload, 256))
