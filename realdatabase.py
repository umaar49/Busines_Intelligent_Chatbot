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
        
        # Ensure it's using mysql+pymysql:// format
        if connection_string.startswith('mysql://'):
            connection_string = connection_string.replace('mysql://', 'mysql+pymysql://', 1)
        
        # Remove any ssl-mode parameters and use proper SSL
        connection_string = connection_string.replace('?ssl-mode=REQUIRED', '').replace('&ssl-mode=REQUIRED', '')
        
        # Add SSL parameters for Aiven
        engine = create_engine(
            connection_string,
            connect_args={
                "ssl": {"ssl": True}  # Simple SSL enable
            },
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=30,
            pool_size=2,
            max_overflow=3,
            echo=False
        )
        
        # Test connection
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.success("✅ Connected to Aiven database successfully!")
        except Exception as e:
            st.error(f"❌ Database connection test failed: {e}")
            # Try without SSL as fallback
            st.info("🔄 Trying without SSL...")
            try:
                engine_no_ssl = create_engine(
                    connection_string,
                    pool_pre_ping=True,
                    pool_recycle=1800,
                    pool_timeout=30,
                    pool_size=2,
                    max_overflow=3,
                    echo=False
                )
                with engine_no_ssl.connect() as conn:
                    conn.execute(text("SELECT 1"))
                st.success("✅ Connected to Aiven database without SSL!")
                return engine_no_ssl
            except Exception as e2:
                st.error(f"❌ Connection without SSL also failed: {e2}")
                return None
            
        return engine
        
    except Exception as e:
        st.error(f"❌ Database engine creation failed: {e}")
        return None


def create_database():
    """Not needed for Aiven - database is already created"""
    return True


def store_user_data(df, table_name="sales_data", user_id=None):
    """Store data with primary key for Aiven compatibility"""
    if not user_id:
        st.error("❌ User ID is required.")
        return False

    # Get table name
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

        # Create a copy of the dataframe and add a primary key column
        df_with_pk = df.copy()
        
        # Add an auto-increment primary key column
        df_with_pk['id'] = range(1, len(df_with_pk) + 1)
        
        # Store data with primary key
        df_with_pk.to_sql(user_table, engine, if_exists='replace', index=False)
        
        # If the table has an 'id' column already, set it as primary key
        try:
            with engine.connect() as conn:
                # Check if we need to alter the table to add primary key
                conn.execute(text(f"ALTER TABLE {user_table} ADD PRIMARY KEY (id)"))
        except:
            # If primary key already exists or can't be set, continue
            pass
            
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
            st.error("❌ Database connection failed")
            return False

        with engine.connect() as conn:
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
