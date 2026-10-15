from options_analytics.migrate import split_statements, sql_files


def test_sql_files_are_packaged_and_ordered() -> None:
    names = [name for name, _ in sql_files("clickhouse")]
    assert names == sorted(names)
    assert names[0] == "001_option_ticks.sql"
    assert [name for name, _ in sql_files("postgres")] == ["001_reference.sql"]


def test_split_statements_ignores_comment_lines() -> None:
    sql = (
        "-- a comment; with a semicolon\nCREATE TABLE a (x Int8);\n\n-- another\nCREATE VIEW b AS SELECT 1;\n"
    )
    assert split_statements(sql) == ["CREATE TABLE a (x Int8)", "CREATE VIEW b AS SELECT 1"]
