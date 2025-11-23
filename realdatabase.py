# realdatabase.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os

@st.cache_resource
def get_engine():
    """Create SQLAlchemy engine for Aiven MySQL (tries SSL CA paths, falls back).
       Expects AIVEN_DATABASE_URL in st.secrets"""
    try:
        if "AIVEN_DATABASE_URL" not in st.secrets:
            st.error("❌ AIVEN_DATABASE_URL not found in Streamlit secrets (Settings → Secrets).")
            return None

        raw_url = st.secrets["AIVEN_DATABASE_URL"]

        # Ensure PyMySQL dialect
        if raw_url.startswith("mysql://"):
            url = raw_url.replace("mysql://", "mysql+pymysql://", 1)
        else:
            url = raw_url

        # Remove explicit ssl-mode parameter if present (we will pass ssl via connect_args)
        url = url.split("?")[0]

        # Try a couple of common CA bundle paths used on Linux containers
        ca_candidates = [
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/ssl/cert.pem",
            "/usr/local/share/ca-certificates/ca-bundle.crt"
        ]

        last_exc = None
        for ca_path in ca_candidates:
            if os.path.exists(ca_path):
                try:
                    engine = create_engine(
                        url,
                        connect_args={"ssl": {"ca": ca_path}},
                        pool_pre_ping=True,
                        pool_recycle=1800,
                        pool_timeout=30,
                        pool_size=3,
                        max_overflow=5,
                        echo=False
                    )
                    # quick test
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    st.success(f"✅ Connected to Aiven DB using CA: {ca_path}")
                    return engine
                except Exception as e:
                    last_exc = e
                    # try next CA candidate

        # If none of CA files existed or all attempts failed, try with ssl param (generic) - some envs accept this
        try:
            engine = create_engine(
                url,
                connect_args={"ssl": {"ssl": True}},
                pool_pre_ping=True,
                pool_recycle=1800,
                pool_timeout=30,
                pool_size=3,
                max_overflow=5,
                echo=False
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.success("✅ Connected to Aiven DB using generic ssl connect_args.")
            return engine
        except Exception as e_generic:
            last_exc = e_generic

        # Final fallback: try without SSL (not recommended) — keep this for troubleshooting only
        try:
            engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_recycle=1800,
                pool_timeout=30,
                pool_size=2,
                max_overflow=3,
                echo=False
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.warning("⚠️ Connected to Aiven DB WITHOUT SSL (fallback). Consider fixing SSL settings.")
            return engine
        except Exception as e_no_ssl:
            last_exc = e_no_ssl

        st.error(f"❌ All connection attempts failed. Last error: {last_exc}")
        return None

    except Exception as e:
        st.error(f"❌ Database engine creation failed: {e}")
        return None


def _sql_type_for_series(series: pd.Series):
    """Map pandas dtype to a simple SQL type string for CREATE TABLE"""
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(series):
        return "TINYINT"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"
    # fallback for objects and others
    return "TEXT"


def create_table_with_pk(engine, table_name: str, df: pd.DataFrame):
    """Create a SQL table with an auto-increment primary key 'id' and columns based on df.
       If table exists, do nothing."""
    try:
        # Check existence
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t"),
                {"t": table_name}
            ).fetchone()
            if exists:
                return True

        # Build CREATE TABLE SQL
        cols_defs = []
        for col in df.columns:
            # avoid SQL reserved words or spaces by quoting column names with backticks
            sql_type = _sql_type_for_series(df[col])
            col_quoted = f"`{col}`"
            cols_defs.append(f"{col_quoted} {sql_type}")

        # Prepend primary key id
        create_sql = f"""
        CREATE TABLE `{table_name}` (
            id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            {', '.join(cols_defs)}
        ) ENGINE=InnoDB;
        """

        with engine.begin() as conn:
            conn.execute(text(create_sql))

        return True

    except Exception as e:
        st.error(f"❌ Failed to create table `{table_name}`: {e}")
        return False


def store_user_data(df: pd.DataFrame, table_name="sales_data", user_id=None):
    """Store data for specific user in Aiven MySQL with a PRIMARY KEY on id."""
    if not user_id:
        st.error("❌ User ID required for storage")
        return False

    # Determine actual table name
    if hasattr(st.session_state, 'existing_table') and st.session_state.existing_table:
        user_table = st.session_state.existing_table
    else:
        user_table = f"{user_id}_{table_name}"
        st.session_state.existing_table = user_table

    try:
        engine = get_engine()
        if engine is None:
            st.error("❌ Cannot connect to database")
            return False

        # Create table with primary key if not exists
        created = create_table_with_pk(engine, user_table, df)
        if not created:
            return False

        # Insert rows in chunks to avoid timeouts
        chunk_size = 500  # adjust small for free-tier
        total_rows = len(df)

        # Use pandas.to_sql with if_exists='append' since table was created
        if total_rows == 0:
            st.warning("⚠️ Empty DataFrame, nothing to store.")
            return True

        if total_rows > chunk_size:
            progress_bar = st.progress(0)
            status_text = st.empty()
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size].copy()
                # Ensure column order matches DB: pandas will insert by column names
                # Insert chunk
                chunk.to_sql(user_table, engine, if_exists='append', index=False)
                progress = min((i + chunk_size) / total_rows, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Stored {min(i + chunk_size, total_rows)} of {total_rows} rows...")
            progress_bar.empty()
            status_text.empty()
        else:
            df.to_sql(user_table, engine, if_exists='append', index=False)

        st.success(f"✅ Data saved in table: `{user_table}`")
        return True

    except SQLAlchemyError as e:
        st.error(f"❌ Storage error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ Unexpected storage error: {e}")
        return False


def execute_sql_query(sql_query: str):
    """Execute SQL and return DataFrame (or None on error)"""
    try:
        engine = get_engine()
        if engine is None:
            return None

        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df

    except Exception as e:
        st.error(f"❌ SQL Execution Error: {e}")
        return None


def delete_user_data(user_id, table_name="sales_data"):
    """Drop the user's table (if exists)"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("❌ Database connection failed")
            return False

        user_table = f"{user_id}_{table_name}"
        with engine.begin() as conn:
            row = conn.execute(text("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t"), {"t": user_table}).fetchone()
            if row:
                conn.execute(text(f"DROP TABLE `{user_table}`"))
                st.success(f"✅ Deleted user data: {user_table}")
                return True
            else:
                st.warning(f"⚠️ No data found for user table: {user_table}")
                return True

    except Exception as e:
        st.error(f"❌ Error deleting user data: {e}")
        return False


def delete_all_user_tables(user_id):
    """Drop all tables that start with 'user_id_'"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("❌ Database connection failed")
            return False

        pattern = f"{user_id}_%"
        with engine.begin() as conn:
            rows = conn.execute(text("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE :p"), {"p": pattern}).fetchall()
            if not rows:
                st.warning(f"⚠️ No tables found for user: {user_id}")
                return True
            count = 0
            for r in rows:
                tname = r[0]
                conn.execute(text(f"DROP TABLE `{tname}`"))
                count += 1
            st.success(f"✅ Deleted {count} tables for user: {user_id}")
            return True

    except Exception as e:
        st.error(f"❌ Error deleting user tables: {e}")
        return False


def get_user_tables(user_id):
    """Return list of tables for this user"""
    try:
        engine = get_engine()
        if engine is None:
            return []
        pattern = f"{user_id}_%"
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE :p"), {"p": pattern}).fetchall()
            return [r[0] for r in rows] if rows else []
    except Exception as e:
        st.error(f"❌ Error getting user tables: {e}")
        return []
