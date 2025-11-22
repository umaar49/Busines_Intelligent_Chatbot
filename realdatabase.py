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
            return None
            
        connection_string = st.secrets['AIVEN_DATABASE_URL']
        
        # Ensure it's using mysql+pymysql:// format
        if connection_string.startswith('mysql://'):
            connection_string = connection_string.replace('mysql://', 'mysql+pymysql://', 1)
        
        # Remove any ssl-mode parameters
        connection_string = connection_string.replace('?ssl-mode=REQUIRED', '').replace('&ssl-mode=REQUIRED', '')
        
        engine = create_engine(
            connection_string,
            connect_args={"ssl": {"ssl": True}},
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=30,
            pool_size=2,
            max_overflow=3,
            echo=False
        )
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        st.success("✅ Connected to Aiven database successfully!")
        return engine
        
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None


def create_database():
    """Not needed for Aiven"""
    return True


def store_user_data(df, table_name="sales_data", user_id=None):
    """Store data with PRIMARY KEY - FIXED VERSION"""
    if not user_id:
        st.error("❌ User ID is required.")
        return False

    # Get table name
    user_table = f"{user_id}_{table_name}"
    st.session_state.existing_table = user_table

    try:
        engine = get_engine()
        if engine is None:
            return False

        # STEP 1: Create table with PRIMARY KEY first
        with engine.connect() as conn:
            # Drop table if exists
            conn.execute(text(f"DROP TABLE IF EXISTS {user_table}"))
            
            # Create table with explicit PRIMARY KEY
            create_sql = f"""
            CREATE TABLE {user_table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join([f'`{col}` TEXT' for col in df.columns])}
            )
            """
            conn.execute(text(create_sql))
            
        # STEP 2: Insert data using SQL (not to_sql)
        with engine.connect() as conn:
            for index, row in df.iterrows():
                # Convert all values to strings for TEXT columns
                values = [str(val) if pd.notna(val) else '' for val in row]
                placeholders = ', '.join(['%s'] * len(values))
                columns_str = ', '.join([f'`{col}`' for col in df.columns])
                
                insert_sql = f"INSERT INTO {user_table} ({columns_str}) VALUES ({placeholders})"
                conn.execute(text(insert_sql), values)
            
            conn.commit()
        
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
            return False

        user_table = f"{user_id}_{table_name}"

        with engine.connect() as conn:
            result = conn.execute(
                text("SHOW TABLES LIKE :table_name"),
                {"table_name": user_table}
            ).fetchone()

            if result:
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
            return False

        with engine.connect() as conn:
            result = conn.execute(
                text("SHOW TABLES LIKE :pattern"),
                {"pattern": f"{user_id}_%"}
            ).fetchall()

            if result:
                for table in result:
                    table_name = table[0]
                    conn.execute(text(f"DROP TABLE {table_name}"))
                conn.commit()
                st.success(f"✅ Deleted tables for user: {user_id}")
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
        return []
