"""V3/legacy assignment projections; raw input remains available to callers.

Classification aliases are recovered by their recorded source names, not by
guessing a taxonomy/version from the length of a code. Geography excludes
commuting crosswalks. Multiple work arrangements remain Unknown.
"""

import json
import math

CLASSIFICATIONS = {
    **{f"NAICS_2022_{n}": ("NAICS", f"NAICS_2022_{n}") for n in range(2, 7)},
    "NAICS2": ("NAICS", "NAICS2"),
    "ONET": ("ONET", "ONET"),
    "SOC_2": ("SOC", "SOC_2"),
    "SOC_5": ("SOC", "SOC_5"),
}
GEOGRAPHY = {
    "LOCATION": "location", "CITY_NAME": "city", "STATE": "state_code",
    "STATE_NAME": "state", "COUNTY": "county_code", "COUNTY_NAME": "county",
    "MSA": "msa_code", "MSA_NAME": "msa",
}
REMOTE = {"remote": "Remote", "hybrid": "Hybrid", "hybrid remote": "Hybrid",
          "onsite": "On-site", "on-site": "On-site", "on site": "On-site",
          "not remote": "On-site"}
ANNUAL = ("annual", "annually", "year", "yearly", "per year", "per annum", "annum")


def present(value):
    return value is not None and str(value).strip() not in ("", "[]", "{}", "null")


def members(value):
    if isinstance(value, list):
        return value
    if not present(value):
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else [value]
    except (ValueError, TypeError):
        return [value]


def positive(value):
    try:
        number = float(str(value).replace(",", "").replace("$", ""))
        return number if math.isfinite(number) and number > 0 else None
    except (ValueError, TypeError):
        return None


def project_job(record):
    """Return a new upper-case working row from a Parquet row or API payload."""
    row = {}
    for key, value in record.items():
        key = key.upper()
        if key in row:
            raise ValueError(f"Column names collide after uppercasing: {key}")
        row[key] = value

    def first(*values):
        return next((v for v in values if present(v)), None)

    classifications = [v for v in members(row.get("CLASSIFICATIONS")) if isinstance(v, dict)]
    for target, (system, source) in CLASSIFICATIONS.items():
        match = next((v for v in classifications if v.get("system") == system
                      and source in (v.get("sources") or [])), {})
        for suffix, key in [("", "code"), ("_NAME", "name")]:
            row[target + suffix] = first(row.get(target + suffix), match.get(key))
    # SOC source projections are explicitly SOC, never substituted for O*NET.
    for suffix, source in [("", "V3_SOURCE_SOC_CODE"), ("_NAME", "V3_SOURCE_SOC_NAME")]:
        row["SOC_5" + suffix] = first(row.get("SOC_5" + suffix), row.get(source))
    locations = [v for v in members(row.get("GEOGRAPHY")) if isinstance(v, dict)]
    locations = [v for kind in ("reported_worksite", "parsed_worksite")
                 for v in locations if v.get("kind") == kind]
    for target, field in GEOGRAPHY.items():
        row[target] = first(row.get(target), *(v.get(field) for v in locations))
    for target, source in [("CITY_NAME", "V3_JOB_CITY"), ("STATE_NAME", "V3_JOB_STATE")]:
        row[target] = first(row.get(target), row.get(source))
    for target, source in [("MSA", "MSA_CODES"), ("MSA_NAME", "MSA_NAMES"), ("CITY_NAME", "CITY_NAMES")]:
        values = members(row.get(source))
        row[target] = first(row.get(target), values[0] if len(values) == 1 else None)
    labels = members(first(row.get("REMOTE_TYPES"), row.get("REMOTE_TYPE_NAME")))
    labels = {REMOTE.get(str(v).strip().lower(), "Unknown") for v in labels}
    row["REMOTE_TYPE_NAME"] = next(iter(labels)) if len(labels) == 1 else "Unknown"
    jobs = members(first(row.get("JOB_TYPES"), row.get("EMPLOYMENT_TYPE_NAME")))
    row["EMPLOYMENT_TYPE_NAME"] = json.dumps(jobs) if jobs else None
    row["IS_INTERNSHIP"] = first(row.get("IS_INTERNSHIP"),
        int(any("intern" in str(v).lower() for v in jobs)) if jobs else None)
    for target, source in [("MIN_YEARS_EXPERIENCE", "REQUIRED_EXPERIENCE_MIN_YEARS"),
                           ("MAX_YEARS_EXPERIENCE", "REQUIRED_EXPERIENCE_MAX_YEARS")]:
        row[target] = first(row.get(source), row.get(target))
    row["EDUCATION_LEVELS_NAME"] = first(row.get("EDUCATION_LEVELS_NAME"), row.get("REQUIRED_EDUCATION_MIN"))
    row["BODY"] = first(row.get("BODY"), row.get("HTML_DESCRIPTION_TEXT"))
    annual = str(row.get("ORIGINAL_PAY_PERIOD") or "").strip().lower() in ANNUAL
    currency = str(row.get("SALARY_CURRENCY") or "USD").upper()
    low, high = positive(row.get("SALARY_FROM")), positive(row.get("SALARY_TO"))
    valid_range = not (low is not None and high is not None and low > high)
    for suffix, value in [("_FROM", low), ("_TO", high),
                          ("", (low + high) / 2 if low and high else low or high or positive(row.get("SALARY")))]:
        row["NORMALIZED_SALARY" + suffix] = positive(row.get("NORMALIZED_SALARY" + suffix)) or (
            value if annual and currency == "USD" and valid_range else None)
    return row


def project_jobs_frame(frame):
    """Native Spark projection of the same contract, without Python row UDFs."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, MapType, StructType

    names = [name.upper() for name in frame.columns]
    if len(names) != len(set(names)):
        raise ValueError("Column names collide after uppercasing")
    frame = frame.toDF(*names)
    types = {field.name: field.dataType for field in frame.schema}

    def text(name):
        if name not in types:
            return F.lit(None).cast("string")
        value = F.col(name)
        if isinstance(types[name], (ArrayType, MapType, StructType)):
            value = F.to_json(value)
        else:
            value = value.cast("string")
        return F.when(~F.trim(value).isin("", "[]", "{}", "null"), value)

    def array(name):
        value = text(name)
        return F.coalesce(F.from_json(value, "array<string>"), F.when(value.isNotNull(), F.array(value)), F.array().cast("array<string>"))

    def first(values):
        return F.get(F.filter(values, lambda v: v.isNotNull() & (F.trim(v) != "")), 0)

    cls = F.from_json(text("CLASSIFICATIONS"), "array<struct<system:string,code:string,name:string,sources:array<string>>>")
    additions = {}
    for target, (system, source) in CLASSIFICATIONS.items():
        matches = F.filter(cls, lambda v: (v.system == system) & F.array_contains(v.sources, source))
        for suffix, field in [("", "code"), ("_NAME", "name")]:
            additions[target + suffix] = F.coalesce(text(target + suffix), first(F.transform(matches, lambda v: v[field])))
    additions["SOC_5"] = F.coalesce(additions["SOC_5"], text("V3_SOURCE_SOC_CODE"))
    additions["SOC_5_NAME"] = F.coalesce(additions["SOC_5_NAME"], text("V3_SOURCE_SOC_NAME"))
    geo = F.from_json(text("GEOGRAPHY"), "array<struct<kind:string," + ",".join(f"{v}:string" for v in GEOGRAPHY.values()) + ">>")
    for target, field in GEOGRAPHY.items():
        values = [first(F.transform(F.filter(geo, lambda v: v.kind == kind), lambda v: v[field]))
                  for kind in ("reported_worksite", "parsed_worksite")]
        additions[target] = F.coalesce(text(target), *values)
    for target, source in [("CITY_NAME", "V3_JOB_CITY"), ("STATE_NAME", "V3_JOB_STATE")]:
        additions[target] = F.coalesce(additions[target], text(source))
    for target, source in [("MSA", "MSA_CODES"), ("MSA_NAME", "MSA_NAMES"), ("CITY_NAME", "CITY_NAMES")]:
        values = array(source)
        additions[target] = F.coalesce(additions[target], F.when(F.size(values) == 1, F.get(values, 0)))
    mapping = F.create_map(*[F.lit(v) for pair in REMOTE.items() for v in pair])
    labels = F.when(text("REMOTE_TYPES").isNotNull(), array("REMOTE_TYPES")).otherwise(array("REMOTE_TYPE_NAME"))
    labels = F.array_distinct(F.transform(labels, lambda v: F.coalesce(mapping[F.lower(F.trim(v))], F.lit("Unknown"))))
    additions["REMOTE_TYPE_NAME"] = F.when(F.size(labels) == 1, F.get(labels, 0)).otherwise(F.lit("Unknown"))
    jobs = F.when(text("JOB_TYPES").isNotNull(), array("JOB_TYPES")).otherwise(array("EMPLOYMENT_TYPE_NAME"))
    additions["EMPLOYMENT_TYPE_NAME"] = F.when(F.size(jobs) > 0, F.to_json(jobs))
    additions["IS_INTERNSHIP"] = F.coalesce(text("IS_INTERNSHIP"), F.when(F.size(jobs) > 0, F.exists(jobs, lambda v: F.lower(v).contains("intern")).cast("int").cast("string")))
    for target, source in [("MIN_YEARS_EXPERIENCE", "REQUIRED_EXPERIENCE_MIN_YEARS"), ("MAX_YEARS_EXPERIENCE", "REQUIRED_EXPERIENCE_MAX_YEARS")]:
        additions[target] = F.coalesce(text(source), text(target))
    additions["EDUCATION_LEVELS_NAME"] = F.coalesce(text("EDUCATION_LEVELS_NAME"), text("REQUIRED_EDUCATION_MIN"))
    additions["BODY"] = F.coalesce(text("BODY"), text("HTML_DESCRIPTION_TEXT"))
    # Materialize aliases in one projection; keep the wide original frame lazy.
    frame = frame.select(*[F.col(c) for c in frame.columns if c not in additions], *[v.alias(k) for k, v in additions.items()])
    # Clean only salary inputs; preserve raw salary columns for source audits.
    for name in ["SALARY", "SALARY_FROM", "SALARY_TO", "NORMALIZED_SALARY", "NORMALIZED_SALARY_FROM", "NORMALIZED_SALARY_TO"]:
        value = F.expr(f"try_cast(regexp_replace(`{name}`, '[$,]', '') as double)") if name in types else F.lit(None).cast("double")
        frame = frame.withColumn("_v3_" + name, F.when((value > 0) & (value < float("inf")) & ~F.isnan(value), value))
    annual = F.lower(F.trim(text("ORIGINAL_PAY_PERIOD"))).isin(*ANNUAL) & (F.upper(F.coalesce(text("SALARY_CURRENCY"), F.lit("USD"))) == "USD")
    low, high = F.col("_v3_SALARY_FROM"), F.col("_v3_SALARY_TO")
    valid = low.isNull() | high.isNull() | (low <= high)
    midpoint = F.coalesce((low + high) / 2, low, high, F.col("_v3_SALARY"))
    for suffix, value in [("_FROM", low), ("_TO", high), ("", midpoint)]:
        name = "NORMALIZED_SALARY" + suffix
        frame = frame.withColumn(name, F.coalesce(F.col("_v3_" + name), F.when(annual & valid, value)))
    return frame.drop(*[c for c in frame.columns if c.startswith("_v3_")])
