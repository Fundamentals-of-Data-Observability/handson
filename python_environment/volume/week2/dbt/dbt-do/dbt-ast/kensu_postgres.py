import datetime
import decimal
import logging
import os
import re
from kensu.psycopg2.pghelpers import get_table_schema, get_current_db_info, pg_query_as_dicts
from kensu.utils.kensu_provider import KensuProvider
from kensu.utils.kensu import KensuDatasourceAndSchema
from kensu.utils.dsl.extractors.external_lineage_dtos import GenericComputedInMemDs

PG_CreateTableAs = 'PG_CreateTableAs'
PG_InsertTable = 'PG_InsertTable'
PG_CreateView = 'PG_CreateView'

# FIXME: quoted vs not quoted table names? e.g.:
# "testme"."testme_schema"."my_first_dbt_model" vs testme.testme_schema.my_first_dbt_model
def pg_relation_to_kensu_table_name(rel):
    if str(type(rel)) == "<class 'dbt.adapters.postgres.relation.PostgresRelation'>":
        return '.'.join([
            rel.database,
            rel.schema,
            rel.identifier
        ])
    return None


def format_relation_name(relation, cur_catalog=None, cur_schema=None):
    if isinstance(relation, dict):
        catalogname = ('catalogname' in relation and relation["catalogname"]) or None
        schemaname = ('schemaname' in relation and relation["schemaname"]) or None
        relname = ('relname' in relation and relation["relname"]) or None
    else:
        catalogname = hasattr(relation, 'catalogname') and relation.catalogname or None
        schemaname = hasattr(relation, 'schemaname') and relation.schemaname or None
        relname = hasattr(relation, 'relname') and relation.relname or None

    if not relname:
        return None
    else:
        parts = [
            catalogname or cur_catalog,
            schemaname or cur_schema,
            relname
        ]
        return '.'.join([n for n in parts if n])


def fetch_input_output_tables(stmt, cur_catalog, cur_schema):
    from pglast import Node
    output_tables = []
    input_tables = []
    for node in Node(stmt.stmt).traverse():
        logging.debug("sql tree entry: "+str(node))
        is_read_node = str(node) == 'fromClause[0]={RangeVar}'
        is_read_from_join_node = str(node) == 'fromClause[0]={JoinExpr}'
        is_write_node = 'rel={RangeVar}' in str(node)
        if is_read_node or is_write_node:
            table_name = format_relation_name(node.ast_node, cur_catalog, cur_schema)
            if is_read_node:
                input_tables.append(table_name)
            elif is_write_node:
                output_tables.append(table_name)
        if is_read_from_join_node:
            for joined_node in [node.ast_node.larg, node.ast_node.rarg]:
                table_name = format_relation_name(joined_node, cur_catalog, cur_schema)
                if table_name:
                    input_tables.append(table_name)
    return input_tables, output_tables

def parse_pg_query(cursor, sql):
    from pglast import parse_sql, ast
    from kensu.psycopg2.pghelpers import get_table_schema, get_current_db_info
    # fixme: this would be needed only if we support non-fully-qualified table references in SQL
    #  cur_catalog, cur_schema = get_current_db_info(cursor)
    cur_catalog, cur_schema = None, None
    parsed_tree = parse_sql(sql)

    for stmt in parsed_tree:
        stmt_type = None
        output_tables = []
        input_tables = []
        if isinstance(stmt, ast.RawStmt):
            if isinstance(stmt.stmt, ast.CreateTableAsStmt):
                stmt_type = PG_CreateTableAs
                input_tables, output_tables = fetch_input_output_tables(stmt, cur_catalog, cur_schema)
            if isinstance(stmt.stmt, ast.InsertStmt):
                stmt_type = PG_InsertTable
                # TODO... there is probably more cases than insert ... values
                table_name = format_relation_name(stmt.stmt.relation(), cur_catalog, cur_schema)
                output_tables.append(table_name)
            if isinstance(stmt.stmt, ast.ViewStmt):
                stmt_type = PG_CreateView
                output_table_name = format_relation_name(stmt.stmt.view, cur_catalog, cur_schema)
                output_tables = [output_table_name]
                input_tables, _ = fetch_input_output_tables(stmt, cur_catalog, cur_schema)
    return stmt_type, input_tables, output_tables


def pg_try_get_schema(cursor, tname):
    # FIXME: move this to kensu-py
    try:
        return list([(f.get('field_name') or 'unknown', f.get('field_type') or 'unknown')
                     for f in get_table_schema(cursor, tname)])
    except:
        logging.warning(f"failed getting schema for Postgres table {tname}")


def pg_to_kensu_entry(kensu_inst, cursor, tname, compute_stats=True):
    # for postgres & mysql, dbt creates temporary tables and rename them later
    # we want the final table name in kensu
    # FIXME: this renaming might cause issues when fetching schema!!! should it happen here?
    cleaned_tname = tname.replace('__dbt_tmp', '')

    maybe_schema = pg_try_get_schema(cursor=cursor, tname=tname)
    logging.warning(f"pg_schema: {maybe_schema}")
    stats_values = None
    if compute_stats and maybe_schema:
        # FIXME: use a fresh cursor?
        stats_values = query_stats(cursor, schema_fields=maybe_schema, orig_tname=tname)
        logging.info(f'final Postgres stats values: {stats_values}')
    server_info = cursor.connection.info.dsn_parameters
    # FIXME: make sure the Postgres URI is consistent among all different collectors
    #  (e.g. is port always explicit vs default port)
    ds_path = f"postgres://{server_info['host']}:{server_info['port']}/{cleaned_tname}"  # FIXME?

    entry = KensuDatasourceAndSchema.for_path_with_opt_schema(
        ksu=kensu_inst,
        ds_path=ds_path,  # FIXME?
        ds_name=cleaned_tname,  # FIXME?
        format='Postgres table',
        # FIXME: ds_path like postgres://localhost:5432/a.b.c seem to cause error in Kensu webui
        # categories=['logical::'+ f"postgres :: {server_info['host']}:{server_info['port']} :: {cleaned_tname}"],
        categories=['logical::' + f"{cleaned_tname}"],
        maybe_schema=maybe_schema,
        f_get_stats=lambda: stats_values
    )  # type: KensuDatasourceAndSchema

    return entry


def report_postgres(conn_mngr, cursor, sql, bindings):
    if bindings is not None:
        # Also we process the "%s" with `bindings` => otherwise the pglast parser fails
        # so the result is
        num_to_replace = len(re.findall("%.", sql))
        sql = sql.replace("%s", "{}").format(*range(0, num_to_replace)) # bindings)

    from kensu_reporting import get_kensu_agent
    kensu_inst = get_kensu_agent()
    
    stmt_type, input_tables, output_tables = parse_pg_query(cursor=cursor, sql=sql)
    # e.g. input_tables=['source_data'], output_tables=['testme.testme_schema.my_first_dbt_model__dbt_tmp']
    # input might contain false `table names`, being the subquery aliases inside the SQL `WITH` statement, e.g.:
    # WITH source_data as (select 1) select * from source_data
    # P.S. for now only fully-qualified table names supported in SQL (e.g. to remove subquery aliases)
    convert_valid_tables_and_fetch_stats_fn = lambda tables: [
        pg_to_kensu_entry(kensu_inst, cursor, t)
        for t in tables if t.count('.') == 2]
    if stmt_type == PG_CreateTableAs:
        logging.info(f'POSTGRES create table. SQL: {sql}')
        all_kensu_inputs = convert_valid_tables_and_fetch_stats_fn(input_tables)
    elif stmt_type == PG_InsertTable:
        # Considering that we are currently in a model inserting from `seed`
        from kensu_reporting import get_current_thread_seeds
        seed_inputs = get_current_thread_seeds()
        inputs = convert_valid_tables_and_fetch_stats_fn(input_tables)
        # TODO probably other cases than just seed_inputs?
        all_kensu_inputs = [*seed_inputs, *inputs]
        logging.info(f'POSTGRES insert. SQL: {sql}')
    elif stmt_type == PG_CreateView:
        all_kensu_inputs = convert_valid_tables_and_fetch_stats_fn(input_tables)
        logging.debug(f'POSTGRES create view. SQL: {sql}')
    else:
        logging.info(f"POSTGRES untracked statement: sql={sql}")
        return
    
    if all_kensu_inputs and output_tables:
        outputs = [pg_to_kensu_entry(kensu_inst, cursor, o)
                    for o in output_tables if o.count('.') == 2]
        for output in outputs:
            lineage=GenericComputedInMemDs.for_direct_or_full_mapping(all_inputs=all_kensu_inputs,
                                                                        out_field_names=output.field_names())
            if len(lineage.lineage) <= 0:
                continue
            lineage.report(
                ksu=kensu_inst,
                df_result=output,
                operation_type='NA',
                report_output=True,
                register_output_orig_data=True
            )
            kensu_inst.report_with_mapping()


def query_stats(cursor, schema_fields, orig_tname):
    stats_aggs = pg_generate_fallback_stats_queries(schema_fields)
    input_filters=None
    filters = ''
    if input_filters is not None and len(input_filters) > 0:
        filters = f"WHERE {' AND '.join(input_filters)}"
    selector = ",".join([sql_aggregation + " " + col.replace(".","__ksu__") + "_" + stat_name
                         for col, stats_for_col in stats_aggs.items()
                         for stat_name, sql_aggregation in stats_for_col.items()])
    stats_sql = f"select {selector}, sum(1) as nrows from {str(orig_tname)} {filters}"
    logging.info(f'SQL query to fetch Postgres stats: {stats_sql}')
    stats_result = pg_query_as_dicts(cur=cursor, q=stats_sql)
    logging.debug(f'Postgres stats: {stats_result}')
    r = {}
    # FIXME: hmm this logic seem quite shared with BigQuery, extract common parts?
    for row in stats_result:
        if row.get('nrows'):
            r['nrows'] = row['nrows']
        # extract column specific stats
        for col, stat_names in stats_aggs.items():
            for stat_name in stat_names.keys():
                result_column = col.replace(".","__ksu__") + "_" + stat_name
                # looks like postgres:13 return only lowercase
                v = row.get(result_column.lower()) or row.get(result_column)
                if v.__class__ in [datetime.date, datetime.datetime, datetime.time]:
                    v = int(v.strftime("%s") + "000")
                if v.__class__ in [decimal.Decimal]:
                    v = float(v)
                if v is None:
                    # FIXME: this might be misleading actually
                    v = 0
                r[(col + "." + stat_name).replace("__ksu__",".")] = v
        break  # there should be only one row here
    return r


def pg_generate_fallback_stats_queries(schema_fields):
    stats_aggs = {}
    for field_name, field_type in schema_fields:
        field_type = field_type.upper()
        # https://www.postgresql.org/docs/9.5/datatype.html
        # seem like we need to quote field names which are case-sensitive (upercase)
        nullrows_agg = f"""sum(num_nulls("{field_name}"))"""
        min_agg = f"""min("{field_name}")"""
        max_agg = f"""max("{field_name}")"""
        avg_agg = f"""avg("{field_name}")"""
        if field_type in ["INTEGER", "INT", "INT4", "DECIMAL", "SMALLINT", "INT2", "FLOAT",
                          "FLOAT4", "FLOAT8", "FLOAT64", "REAL", "NUMERIC", "BIGINT", "INT8"]:
            stats_aggs[field_name] = {"min": min_agg,
                                      "max": max_agg,
                                      "mean": avg_agg,
                                      "nullrows": nullrows_agg}
        elif field_type in ["TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME", "TIMETZ", "DATETIME"]:
            stats_aggs[field_name] = {"min": min_agg,
                                      "max": max_agg,
                                      "nullrows": nullrows_agg}
        elif field_type in ["BOOLEAN", "BOOL"]:
            stats_aggs[field_name] = {"true": f"""sum(case "{field_name}" when true then 1 else 0 end)""",
                                      "nullrows": nullrows_agg}
        elif field_type in ["STRING", "TEXT"]:
            stats_aggs[field_name] = {"levels": f"""count(distinct "{field_name}")""",
                                      "nullrows": nullrows_agg}

    return stats_aggs
