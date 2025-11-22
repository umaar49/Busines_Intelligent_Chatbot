import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


@st.cache_resource
def get_engine():
    """Create database engine using Streamlit secrets"""
    try:
        # Get database URL from Streamlit secrets
        if 'AIVEN_DATABASE_URL' not in st.secrets:
            st.error("❌ AIVEN_DATABASE_URL not found in Streamlit secrets")
            st.info("💡 Please add your Aiven database URL to Streamlit secrets")
            return None
            
        connection_string = st.secrets['AIVEN_DATABASE_URL']
        
        # Ensure SSL is properly configured for Aiven
        if "ssl-mode=REQUIRED" not in connection_string:
            connection_string += "?ssl-mode=REQUIRED"
        
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=30,
            pool_size=3,           # Smaller for Aiven free tier
            max_overflow=5,
            echo=False
        )
        
        # Test connection with simpler approach
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.success("✅ Connected to Aiven database successfully!")
        except Exception as e:
            st.error(f"❌ Database connection test failed: {e}")
            return None
            
        return engine
        
    except Exception as e:
        st.error(f"❌ Database engine creation failed: {e}")
        return None


def create_database():
    """Not needed for Aiven - database is already created"""
    # Aiven creates the database automatically, just return True
    return True


def store_user_data(df, table_name="sales_data", user_id=None):
    """Store data with chunking to avoid timeouts"""
    if not user_id:
        st.error("❌ User ID is required.")
        return False

    # Get table name from session state
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

        # CHUNK DATA TO PREVENT TIMEOUTS (smaller chunks for Aiven)
        chunk_size = 200  # Even smaller for Aiven free tier
        total_rows = len(df)

        if total_rows > chunk_size:
            progress_bar = st.progress(0)
            status_text = st.empty()

            # Create table with first chunk
            df.head(0).to_sql(user_table, engine, if_exists='replace', index=False)

            # Insert remaining data in chunks
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                chunk.to_sql(user_table, engine, if_exists='append', index=False)

                # Update progress
                progress = min((i + chunk_size) / total_rows, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Stored {min(i + chunk_size, total_rows)} of {total_rows} rows...")

            progress_bar.empty()
            status_text.empty()
        else:
            # Small dataset - single operation
            df.to_sql(user_table, engine, if_exists='replace', index=False)

        st.success(f"✅ Data saved in table: `{user_table}`")
        return True

    except Exception as e:
        st.error(f"❌ Storage error: {e}")
        return False


def execute_sql_query(sql_query):
    """Run SQL query and return a dataframe"""
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
    """Delete user's data from database"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("❌ Database connection failed")
            return False

        # Construct the actual table name used in storage
        user_table = f"{user_id}_{table_name}"

        with engine.connect() as conn:
            # Check if table exists before trying to delete
            result = conn.execute(
                text("SHOW TABLES LIKE :table_name"),
                {"table_name": user_table}
            ).fetchone()

            if result:
                # Table exists - drop it
                conn.execute(text(f"DROP TABLE {user_table}"))
                conn.commit()
                st.success(f"✅ Deleted user data: {user_table}")
                return True
            else:
                st.warning(f"⚠️ No data found for user: {user_id}")
                return True

    except Exception as e:
        st.error(f"❌ Error deleting user data: {e}")
        return False


def delete_all_user_tables(user_id):
    """Delete ALL tables for a specific user"""
    try:
        engine = get_engine()
        if engine is None:
            st.error("❌ Database connection failed")
            return False

        with engine.connect() as conn:
            # Find all tables for this user
            result = conn.execute(
                text("SHOW TABLES LIKE :pattern"),
                {"pattern": f"{user_id}_%"}
            ).fetchall()

            if result:
                tables_deleted = 0
                for table in result:
                    table_name = table[0]
                    conn.execute(text(f"DROP TABLE {table_name}"))
                    tables_deleted += 1
                    st.info(f"🗑️ Deleted table: {table_name}")

                conn.commit()
                st.success(f"✅ Deleted {tables_deleted} tables for user: {user_id}")
                return True
            else:
                st.warning(f"⚠️ No tables found for user: {user_id}")
                return True

    except Exception as e:
        st.error(f"❌ Error deleting user tables: {e}")
        return False


def get_user_tables(user_id):
    """Get list of all tables belonging to a user"""
    try:
        engine = get_engine()
        if engine is None:
            return []

        with engine.connect() as conn:
            result = conn.execute(
                text("SHOW TABLES LIKE :pattern"),
                {"pattern": f"{user_id}_%"}
            ).fetchall()

            return [table[0] for table in result] if result else []

    except Exception as e:
        st.error(f"Error getting user tables: {e}")
        return []
