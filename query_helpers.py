import os
import re
from io import StringIO
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

import pandas as pd
import duckdb
from simple_salesforce import SalesforceLogin, Salesforce


def salesforce_auth():
    sf_user = os.getenv("SF_USER")
    sf_pass = os.getenv("SF_PASS")
    session_id, sf_instance = SalesforceLogin(username=sf_user, password=sf_pass)
    sf = Salesforce(session_id=session_id, instance=sf_instance)
    return sf

def motherduck_auth():
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    con = duckdb.connect(f"md:?motherduck_token={motherduck_token}")
    return con

def rest_query_to_df(sf, soql, include_deleted: bool = False) -> pd.DataFrame:

    def _split_select_fields(select_block: str) -> list[str]:
        # Split on commas, but respect parentheses and quotes
        fields, buf, depth, in_s, in_d = [], [], 0, False, False
        for ch in select_block:
            if ch == "'" and not in_d:
                in_s = not in_s
            elif ch == '"' and not in_s:
                in_d = not in_d
            elif not (in_s or in_d):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth = max(0, depth - 1)
                elif ch == ',' and depth == 0:
                    fields.append(''.join(buf).strip())
                    buf = []
                    continue
            buf.append(ch)
        if buf:
            fields.append(''.join(buf).strip())
        return [f for f in fields if f]

    def _relationship_parents_from_soql(soql_text: str) -> set[str]:
        m = re.search(r"\bselect\b(.*?)\bfrom\b", soql_text, flags=re.I | re.S)
        if not m:
            return set()
        raw_fields = _split_select_fields(m.group(1))
        parents: set[str] = set()
        for f in raw_fields:
            # remove alias if present: "Owner.Name aliasName"
            f_main = f.split() [0]
            parts = f_main.split(".")
            if len(parts) > 1:
                for i in range(1, len(parts)):
                    parents.add(".".join(parts[:i]))
        return parents

    rel_parents = _relationship_parents_from_soql(soql)

    def _flatten_record(record: dict, parent_key: str = "", sep: str = ".") -> dict:
        items = []
        for key, value in record.items():
            if key == "attributes":
                continue
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, (dict, OrderedDict)):
                # Recurse into nested objects
                items.extend(_flatten_record(value, new_key, sep).items())
            else:
                if value is None and new_key in rel_parents:
                    continue
                items.append((new_key, value))
        return dict(items)

    records = []
    for rec in sf.query_all_iter(query=soql, include_deleted=include_deleted):
        records.append(_flatten_record(rec))

    df = pd.DataFrame(records)

    return df

def bulk_query_to_df(sf, soql):
    match = re.search(r"\bFROM\s+([a-zA-Z0-9_]+)", soql, flags=re.IGNORECASE)
    if not match:
        raise ValueError("No FROM clause found in the query!")
    object_name = match.group(1)
    bulk_object = getattr(sf.bulk2, object_name)
    csv_lines = list(bulk_object.query_all(soql))
    csv_data = "".join(csv_lines)
    df = pd.read_csv(StringIO(csv_data), low_memory=False)
    cols = pd.Series(df.columns, index=df.columns).astype(str).str.strip()
    header_like = df.astype(str).apply(lambda s: s.str.strip()).eq(cols).all(axis=1)
    df = df.loc[~header_like].reset_index(drop=True)
    return df

def run_sf_query(sf, soql, use_api="rest", include_deleted=True, add_ingested_col=True):
    if use_api == "bulk":
        print("Using Bulk API")
        df = bulk_query_to_df(sf, soql)
    elif use_api == "rest":
        print("Using REST API")
        df = rest_query_to_df(sf, soql, include_deleted=include_deleted)
    else:
        raise ValueError("use_api must be 'rest' or 'bulk'")
    if "SystemModstamp" in df.columns:
        df["SystemModstamp"] = (
            pd.to_datetime(df["SystemModstamp"], utc=True, errors="coerce")
            .dt.tz_localize(None)
        )
    if add_ingested_col:
        df["ingested_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    return df

def soql_to_ddl(sf, soql, table_name=None, add_ingested_at=True, quote_identifiers=True):
    def _extract_select_list(soql):
        m = re.search(r"select\s+(.*?)\s+from\s", soql, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            raise ValueError("Could not parse SELECT ... FROM in SOQL.")
        select_body = m.group(1).strip()
        items, buf, depth = [], [], 0
        for ch in select_body:
            if ch == '(':
                depth += 1
                buf.append(ch)
            elif ch == ')':
                depth = max(depth - 1, 0)
                buf.append(ch)
            elif ch == ',' and depth == 0:
                token = ''.join(buf).strip()
                if token:
                    items.append(token)
                buf = []
            else:
                buf.append(ch)
        last = ''.join(buf).strip()
        if last:
            items.append(last)
        cleaned = []
        for item in items:
            if re.search(r"\w+\s*\(", item):
                continue
            cleaned.append(item.split()[0])
        return cleaned

    def _extract_from_object(soql):
        m = re.search(r"\sfrom\s+([a-zA-Z0-9_]+)\b", soql, flags=re.IGNORECASE)
        if not m:
            raise ValueError("Could not parse FROM object in SOQL.")
        return m.group(1)

    describe_cache = {}

    def _global_sobject_name(name_guess):
        try:
            if "_GLOBAL_DESCRIBE" not in describe_cache:
                describe_cache["_GLOBAL_DESCRIBE"] = sf.describe()
            gd = describe_cache["_GLOBAL_DESCRIBE"]
            low = name_guess.lower()
            for sobj in gd.get("sobjects", []):
                if sobj.get("name", "").lower() == low:
                    real = sobj["name"]
                    return real
        except Exception:
            pass
        if name_guess.lower().endswith("__c"):
            base = name_guess[:-3]
            parts = base.split('_')
            base_tc = "_".join(p[:1].upper() + p[1:].lower() for p in parts if p)
            real = base_tc + "__c"
            return real
        real = name_guess[:1].upper() + name_guess[1:]
        return real

    def _get_sobject_describe(sobject):
        key = sobject.lower()
        if key in describe_cache:
            desc = describe_cache[key]
            return desc
        api_name = _global_sobject_name(sobject)
        desc = getattr(sf, api_name).describe()
        describe_cache[key] = desc
        return desc

    def _index_fields(desc):
        by_name, by_rel = {}, {}
        for f in desc.get("fields", []):
            if "name" in f:
                by_name[f["name"].lower()] = f
            rn = f.get("relationshipName")
            if rn:
                by_rel[rn.lower()] = f
        pair = (by_name, by_rel)
        return pair

    def _resolve_field_type_on_object(sobject, field_name):
        desc = _get_sobject_describe(sobject)
        by_name, _ = _index_fields(desc)
        f = by_name.get(field_name.lower())
        t = f.get("type", "unknown") if f else "unknown"
        return t

    def _resolve_path_type(sobject, path):
        if not path:
            return "unknown"
        if len(path) == 1:
            t = _resolve_field_type_on_object(sobject, path[0])
            return t
        desc = _get_sobject_describe(sobject)
        by_name, by_rel = _index_fields(desc)
        rel_token = path[0]
        rel_field = by_rel.get(rel_token.lower())
        if not rel_field:
            if rel_token.lower().endswith("__r"):
                rel_field = by_name.get((rel_token[:-3] + "__c").lower())
            if not rel_field:
                rel_field = by_name.get((rel_token + "Id").lower())
        if not rel_field or rel_field.get("type") != "reference":
            return "unknown"
        targets = rel_field.get("referenceTo", []) or []
        remaining = path[1:]
        if not targets:
            return "unknown"
        resolved = {_resolve_path_type(tgt, remaining) for tgt in targets}
        resolved.discard("unknown")
        if not resolved:
            return "unknown"
        t = resolved.pop() if len(resolved) == 1 else "|".join(sorted(resolved))
        return t

    type_map = {
        "id": "VARCHAR(18)",
        "reference": "VARCHAR(18)",
        "junctionidlist": "LIST<VARCHAR(18)>",
        "string": "VARCHAR",
        "textarea": "VARCHAR",
        "email": "VARCHAR",
        "phone": "VARCHAR",
        "url": "VARCHAR",
        "encryptedstring": "VARCHAR",
        "picklist": "VARCHAR",
        "combobox": "VARCHAR",
        "multipicklist": "LIST<VARCHAR>",
        "boolean": "BOOLEAN",
        "int": "INTEGER",
        "long": "BIGINT",
        "double": "DOUBLE",
        "percent": "DOUBLE",
        "currency": "DOUBLE",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "time": "TIME",
        "base64": "BLOB",
        "anytype": "JSON",
        "location": "STRUCT(lat DOUBLE, lon DOUBLE)",
        "address": "JSON",
        "datacategorygroupreference": "VARCHAR",
    }

    def _duck_type(sf_type):
        st = (sf_type or "unknown").lower()
        if "|" in st:
            return "VARCHAR"
        return type_map.get(st, "VARCHAR")

    def _quote_ident(ident):
        if not quote_identifiers:
            q = ident
        else:
            q = '"' + ident.replace('"', '""') + '"'
        return q

    fields = _extract_select_list(soql)
    base_object = _extract_from_object(soql)
    if not table_name:
        table_name = base_object
    expr_to_type = {}
    for expr in fields:
        path = [p.strip() for p in expr.split(".") if p.strip()]
        expr_to_type[expr] = _resolve_path_type(base_object, path)
    ddl_lines = [f"CREATE TABLE {_quote_ident(table_name)} ("]
    for expr, sf_type in expr_to_type.items():
        duck = _duck_type(sf_type)
        ddl_lines.append(f"    {_quote_ident(expr)} {duck},")
    if add_ingested_at:
        ddl_lines.append("    ingested_at TIMESTAMP")
        ddl_lines.append(");")
    else:
        if ddl_lines[-1].endswith(","):
            ddl_lines[-1] = ddl_lines[-1][:-1]
        ddl_lines.append(");")
    ddl = "\n".join(ddl_lines)
    result = (ddl, base_object)
    return result

def incremental_refresh_table(sf, con, table, soql_template):
    # Get high-water mark for table
    current_max = con.sql(f'SELECT max(SystemModstamp) FROM "{table}"').fetchone()[0]
    print(f'[{table}] Current max(SystemModstamp) in target: {current_max}')

    # Add UTC timezone
    current_max = current_max.replace(tzinfo=timezone.utc)

    # Compute lower bound (5-minute safety window)
    lower_bound = current_max - timedelta(minutes=5)

    # Convert to Salesforce timezone format
    lower_bound_sf_format = lower_bound.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Insert lower bound into SOQL
    soql = soql_template.format(lower_bound=lower_bound_sf_format)

    # Query SF for changed rows
    changed_rows = run_sf_query(
        sf,
        soql,
        use_api="rest",
        include_deleted=True,
        add_ingested_col=True,
    )

    # Checks
    if changed_rows.empty:
        print(f'[{table}] No changes since {lower_bound_sf_format}.')
        return 0

    if "SystemModstamp" not in changed_rows.columns:
        raise ValueError("Expected 'SystemModstamp' in the SELECT for incremental loads.")
    if "Id" not in changed_rows.columns:
        raise ValueError("Expected 'Id' in the SELECT for incremental loads.")

    # Upsert into Motherduck table
    con.register("changed_rows_df", changed_rows)
    con.sql("BEGIN")
    try:
        con.sql(f'DELETE FROM "{table}" WHERE Id IN (SELECT Id FROM changed_rows_df)')
        con.sql(f'INSERT INTO "{table}" BY NAME SELECT * FROM changed_rows_df')
        con.sql("COMMIT")
        n = len(changed_rows)
        print(f'[{table}] Upserted {n} rows since {lower_bound_sf_format}.')
        return n
    except Exception:
        con.sql("ROLLBACK")
        raise