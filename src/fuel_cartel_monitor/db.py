"""DuckDB database connection and schema management."""
from pathlib import Path

import duckdb

DEFAULT_DB_PATH = Path("data/fuel_monitor.duckdb")
SQL_DIR = Path(__file__).parent.parent.parent / "sql"


def get_connection(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection, creating the database if needed."""
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    _init_schema(con)
    return con


def _execute_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Execute a SQL file containing multiple statements."""
    sql = path.read_text()
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            con.execute(statement)


def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Initialize database schema, macros, and views."""
    _execute_sql_file(con, SQL_DIR / "schema.sql")
    _execute_sql_file(con, SQL_DIR / "macros.sql")
    _execute_sql_file(con, SQL_DIR / "views.sql")
