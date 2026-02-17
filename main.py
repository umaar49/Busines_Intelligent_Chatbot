import pandas as pd
import streamlit as st
import realdatabase
import google.generativeai as genai
import hashlib
from sqlalchemy import text
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

# Add this right after your imports at the very top
st.set_page_config(page_title="Business Analyst Chatbot", layout="wide")
st.title("🚀 Business Intelligence Dashboard")

# ADD THIS LINE - Prevents unnecessary reruns
st.session_state.setdefault('analysis_done', False)

# -------------------------
# Initialize session state
# -------------------------
defaults = {
    'user_authenticated': False,
    'user_id': None,
    'processed_df': None,
    'user_tables': {},
    'current_dataset': None,
    'user_identifier': None,
    'existing_table': None,
    'new_user_id': None,
    'data_stored': False,
    'permanent_results': None,
    'permanent_question': None,
    'chart_submitted': False  # NEW: Track chart submission
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# -------------------------
# Hybrid Preprocessing Functions
# -------------------------
def complete_preprocessing_pipeline(df):
    """Manual preprocessing fallback"""
    df = df.copy()
    print(f"Shape BEFORE preprocessing: {df.shape}")

    # Handle missing values
    low_missing_cols = []
    high_missing_cols = []

    for col in df.columns:
        missing_ratio = df[col].isna().mean()
        if missing_ratio <= 0.05:
            low_missing_cols.append(col)
        else:
            high_missing_cols.append(col)

    # Drop rows with minimal missing values
    df = df.dropna(subset=low_missing_cols)

    # Fill high-missing columns
    for col in high_missing_cols:
        if df[col].dtype != "object":
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else "")

    # Remove duplicates
    df = df.drop_duplicates()

    print(f"Shape AFTER preprocessing: {df.shape}")
    return df


def gemini_auto_preprocessing(df, api_key):
    """Auto preprocessing using Gemini"""
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("models/gemini-2.5-pro")  # Use available model

        sample = df.head(20).to_string()

        prompt = f"""
You are a data preprocessing expert.

Given this sample DataFrame:

{sample}

Perform the following:
1. Detect & correct datatypes
2. Fix formatting issues
3. Clean numeric/currency/text issues
4. Identify important columns
5. Create engineered features:
   - time base
   - ratios
   - totals
   - aggregrate
6. RETURN ONLY VALID PYTHON CODE that modifies 'df'.
Do NOT include any explanation or backticks.
"""

        response = model.generate_content(prompt)
        code = response.text

        # Clean code (remove markdown formatting)
        code = code.replace("```python", "").replace("```", "").strip()
        return code

    except Exception as e:
        st.error(f"Gemini preprocessing failed: {e}")
        return None


def execute_generated_code(df, generated_code):
    """Execute generated code safely"""
    import pandas as pd
    import numpy as np

    local_vars = {
        "df": df,
        "pd": pd,
        "np": np
    }

    try:
        exec(generated_code, {}, local_vars)
        return local_vars.get("df", df)
    except Exception as e:
        st.warning(f"Gemini code execution failed, using manual preprocessing: {e}")
        return df


def hybrid_preprocessing(df, api_key):
    """Full hybrid preprocessing pipeline"""
    st.info("🔄 Running hybrid preprocessing...")

    # Step 1: Manual preprocessing
    df_clean = complete_preprocessing_pipeline(df)

    # Step 2: Gemini auto preprocessing
    generated_code = gemini_auto_preprocessing(df_clean, api_key)

    if generated_code:
        # Step 3: Execute Gemini transformations
        final_df = execute_generated_code(df_clean, generated_code)
        st.success("✅ Hybrid preprocessing completed!")
        return final_df

    else:
        st.warning("⚠️ Using manual preprocessing only")
        return df_clean


# -------------------------
# Core Functions
# -------------------------
def verify_user_table_exists(user_hash, table_name):
    """Check if user's table exists in database"""
    try:
        full_table_name = f"{user_hash}_{table_name}"
        engine = realdatabase.get_engine()
        if engine is None:
            return False
        with engine.connect() as conn:
            result = conn.execute(text(f"SHOW TABLES LIKE '{full_table_name}'")).fetchone()
        return result is not None
    except Exception as e:
        st.error(f"Error verifying table: {e}")
        return False


def load_dataset(file):
    """Load dataset from uploaded file"""
    if file is None:
        return None
    try:
        if file.name.endswith('.csv'):
            try:
                df = pd.read_csv(file, encoding="utf-8", header=0)
            except UnicodeDecodeError:
                file.seek(0)
                df = pd.read_csv(file, encoding="latin-1", header=0)
        elif file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, header=0)
        elif file.name.endswith('.json'):
            df = pd.read_json(file)
        else:
            st.error("Unsupported file format!")
            return None

        # Fix unnamed columns
        if df.columns.astype(str).str.contains("Unnamed").any():
            st.warning("⚠️ Fixed unnamed columns in dataset")
            if file.name.endswith('.csv'):
                file.seek(0)
                df = pd.read_csv(file, header=None)
            else:
                file.seek(0)
                df = pd.read_excel(file, header=None)
            df.columns = df.iloc[0]
            df = df[1:]

        return df
    except Exception as e:
        st.error(f"Error loading file: {e}")
        return None


def text_to_sql_final(user_query, table_name, columns):
    """SQL Generator using Gemini"""
    try:
        # Configure Gemini - Get API key from secrets
        if 'GEMINI_API_KEY' not in st.secrets:
            st.error("❌ GEMINI_API_KEY not found in Streamlit secrets")
            st.info("💡 Please add your Gemini API key to .streamlit/secrets.toml")
            return None

        genai.configure(api_key=st.secrets['GEMINI_API_KEY'])
        model = genai.GenerativeModel('models/gemini-2.0-flash')

        prompt = f"""
Convert this question to SQL: {user_query}
Table: {table_name}
Columns: {columns}
Return only SQL query:
"""

        response = model.generate_content(prompt)
        sql_query = response.text.strip()

        if "```sql" in sql_query:
            sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
        elif "```" in sql_query:
            sql_query = sql_query.split("```")[1].strip()

        return sql_query

    except Exception as e:
        st.error(f"SQL Generation Error: {e}")
        return None


# Configure Plotly for PNG export
pio.kaleido.scope.default_format = "png"


def create_and_display_chart(df, chart_type, x_axis, y_axis):
    try:
        st.markdown("---")
        st.subheader("📊 Visualization")

        # --- Auto histogram for categorical columns ---
        if df[x_axis].dtype == "object" or df[x_axis].dtype.name == "category":
            if df[x_axis].nunique() > 25:
                chart_type = "Histogram"
                y_axis = None

        # --- Chart Creation ---
        if chart_type == "Bar Chart":
            fig = px.bar(df, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis}",
                         color=y_axis, color_continuous_scale="viridis")

        elif chart_type == "Line Chart":
            fig = px.line(df, x=x_axis, y=y_axis, title=f"{y_axis} Trend by {x_axis}", markers=True)

        elif chart_type == "Pie Chart":
            if len(df) > 15:
                fig = px.bar(df, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis}")
            else:
                fig = px.pie(df, names=x_axis, values=y_axis, title=f"{y_axis} Distribution")

        elif chart_type == "Scatter Plot":
            fig = px.scatter(df, x=x_axis, y=y_axis, title=f"{y_axis} vs {x_axis}")

        elif chart_type == "Histogram":
            fig = px.histogram(df, x=x_axis, title=f"Distribution of {x_axis}")

        elif chart_type == "Box Plot":
            fig = px.box(df, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis}")

        fig.update_layout(height=500, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
        st.success(f"✅ {chart_type} created successfully!")

        # -------------------------
        # DOWNLOAD BUTTONS
        # -------------------------

        # PNG download
        if st.button("Generate PNG", key="generate_png_safe"):
            try:
                png_bytes = fig.to_image(format="png")
                st.download_button(
                    label="📥 Download Chart (PNG)",
                    data=png_bytes,
                    file_name="chart.png",
                    mime="image/png",
                    key="download_png_button"
                )
            except Exception as e:
                st.error(f"PNG export failed: {e}")

        # HTML download
        html_bytes = fig.to_html().encode()
        st.download_button(
            label="📥 Download Chart (Interactive HTML)",
            data=html_bytes,
            file_name="chart.html",
            mime="text/html"
        )

    except Exception as e:
        st.error(f"❌ Error creating chart: {str(e)}")


def auto_detect_axes(df):
    all_cols = df.columns.tolist()
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

    # PRIORITY time columns
    priority_time_cols = ["date", "week", "month", "day", "year"]

    x_axis = None

    # Detect best X-axis based on time
    for keyword in priority_time_cols:
        for col in all_cols:
            if keyword in col.lower():
                x_axis = col
                break
        if x_axis:
            break

    # Fallback for non-time datasets
    if x_axis is None:
        non_numeric_cols = df.select_dtypes(exclude=['number']).columns.tolist()
        if non_numeric_cols:
            x_axis = non_numeric_cols[0]
        else:
            x_axis = all_cols[0]

    # Detect best Y-axis
    preferred_y_keywords = ["sales", "total", "amount", "price", "revenue", "qty", "quantity"]

    y_axis = None

    for col in numeric_cols:
        if col != x_axis and any(k in col.lower() for k in preferred_y_keywords):
            y_axis = col
            break

    if y_axis is None:
        for col in numeric_cols:
            if col != x_axis:
                y_axis = col
                break

    return x_axis, y_axis


def display_results_with_auto_chart(results_df, user_question):
    if results_df.empty:
        st.info("No data to display.")
        return

    st.subheader("📊 Analysis Results")

    # ==========================
    # 📋 DATA TABLE + CSV EXPORT
    # ==========================
    with st.expander("📋 Tabular Data", expanded=True):
        st.dataframe(results_df, use_container_width=True, height=400)

        csv_data = results_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name="raw_data.csv",
            mime="text/csv",
            key="download_csv_button"
        )

    x_axis, y_axis = auto_detect_axes(results_df)

   # TO CHART SELECTION LOGIC (NEW & IMPROVED)
  
    time_keywords = [
        "date", "week", "month", "year", "day", "quarter", "time",
        "period", "daily", "monthly", "weekly", "yearly"
    ]

    trend_keywords = [
        "trend", "forecast", "increase", "growth", "pattern",
        "change", "season", "moving average", "progress"
    ]

    bar_keywords = [
        "compare", "comparison", "distribution", "rank", "top",
        "bottom", "breakdown", "split"
    ]
    user_q = user_question.lower()
    if y_axis:
        if (any(k in x_axis.lower() for k in time_keywords) or
            any(k in user_q for k in time_keywords) or
            any(k in user_q for k in trend_keywords)):
            
            chart_type = "Line Chart"
        elif any(k in user_q for k in bar_keywords):
            chart_type = "Bar Chart"
        else:
            chart_type = "Bar Chart"

    else:
        chart_type = "Histogram"
    create_and_display_chart(results_df, chart_type, x_axis, y_axis)


def show_chatbot_interface():
    """Main chatbot interface for data analysis"""
    st.subheader("🤖 Business Intelligence Chatbot")

    user_table_name = list(st.session_state.user_tables.values())[0]

    # Get columns
    try:
        columns_query = f"SHOW COLUMNS FROM {user_table_name}"
        columns_df = realdatabase.execute_sql_query(columns_query)
        columns_list = columns_df['Field'].tolist() if columns_df is not None else []
    except:
        columns_list = []

    # User input
    user_question = st.text_area(
        "Ask about your data:",
        placeholder="e.g., Show total sales by country, Monthly revenue trend, Top 10 products...",
        height=100,
        key="user_question_input"
    )

    if st.button("🔍 Analyze", type="primary", use_container_width=True, key="analyze_btn"):
        if user_question:
            with st.spinner("Generating SQL query..."):
                sql_query = text_to_sql_final(user_question, user_table_name, columns_list)

            if sql_query:
                with st.spinner("Executing query..."):
                    results_df = realdatabase.execute_sql_query(sql_query)

                if results_df is not None and not results_df.empty:
                    display_results_with_auto_chart(results_df, user_question)
                else:
                    st.error("❌ No results returned from query")
            else:
                st.error("❌ Could not generate SQL query")
        else:
            st.warning("Please enter a question")


def show_file_upload_interface():
    """File upload and automatic processing interface"""
    upload_data = st.file_uploader("📁 Upload your dataset", type=['csv', 'xlsx', 'xls', 'json'])

    if upload_data is not None:
        df = load_dataset(upload_data)

        if df is not None:
            st.session_state.current_dataset = df

            st.subheader("📊 Dataset Preview")
            st.dataframe(df.head(), use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Rows", df.shape[0])
            with col2:
                st.metric("Columns", df.shape[1])

            # Automatic processing and storage
            if st.button("🚀 Process Data", type="primary", key="process_store_btn"):
                with st.spinner("Processing and storing data..."):
                    # Apply hybrid preprocessing - Get API key from secrets
                    if 'GEMINI_API_KEY' not in st.secrets:
                        st.error("❌ GEMINI_API_KEY not found in Streamlit secrets")
                        st.info("💡 Please add your Gemini API key to .streamlit/secrets.toml")
                        return

                    processed_df = hybrid_preprocessing(df, st.secrets['GEMINI_API_KEY'])

                    st.session_state.processed_df = processed_df

                    # Store in database
                    table_name = "business_data"
                    success = realdatabase.store_user_data(processed_df, table_name, st.session_state.user_id)

                    if success:
                        st.session_state.user_tables[table_name] = st.session_state.existing_table
                        st.session_state.data_stored = True
                        st.success("✅ Data processed and stored successfully!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to store data in database")

    # Show chatbot if data is processed
    if st.session_state.processed_df is not None and st.session_state.user_tables:
        st.markdown("---")
        show_chatbot_interface()


def authenticate_user():
    """User authentication system"""
    st.sidebar.header("🔐 User Authentication")

    existing_id = st.sidebar.text_input("Enter your Existing ID:", placeholder="e.g., 16eafc_sales_data")

    st.sidebar.markdown("**Or create new account:**")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        user_name = st.sidebar.text_input("Your Name:", placeholder="umar")
    with col2:
        user_email = st.sidebar.text_input("Your Email:", placeholder="umar@company.com")

    if st.sidebar.button("🚀 Login / Create ID", type="primary", use_container_width=True):
        # Use existing ID
        if existing_id:
            if "_" not in existing_id:
                st.sidebar.error("❌ Invalid format. Should be: abc123_sales_data")
                return False

            user_hash = existing_id.split("_")[0]
            table_name = "_".join(existing_id.split("_")[1:])

            if verify_user_table_exists(user_hash, table_name):
                st.session_state.user_id = user_hash
                st.session_state.user_authenticated = True
                st.session_state.user_identifier = f"Existing: {existing_id}"
                st.session_state.existing_table = existing_id
                st.session_state.user_tables = {table_name: existing_id}
                st.session_state.data_stored = True
                st.sidebar.success(f"✅ Welcome back! Accessing: {existing_id}")
                st.rerun()
            else:
                st.sidebar.error("❌ ID not found. Create new ID or check spelling")
            return False

        # Create new ID
        if user_name and user_email:
            user_identifier = f"{user_name}_{user_email}"
            user_hash = hashlib.md5(user_identifier.encode()).hexdigest()[:6]
            table_name = "sales_data"
            new_user_id = f"{user_hash}_{table_name}"

            st.session_state.user_id = user_hash
            st.session_state.user_authenticated = True
            st.session_state.user_identifier = user_identifier
            st.session_state.new_user_id = new_user_id
            st.session_state.existing_table = new_user_id
            st.session_state.user_tables = {table_name: new_user_id}

            st.sidebar.success(f"🎉 Your new ID: `{new_user_id}`")
            st.sidebar.info("**📝 IMPORTANT:** Save this ID to access your data later!")
            st.rerun()
            return True

        st.sidebar.error("❌ Please enter either Existing ID OR Name+Email")

    # Hints
    if existing_id:
        st.sidebar.info("👆 Click the button above after entering your ID")
    elif user_name or user_email:
        st.sidebar.info("👆 Click the button above to create your ID")

    return st.session_state.user_authenticated


# -------------------------
# MAIN APPLICATION LOGIC
# -------------------------

# Authentication check
if not st.session_state.user_authenticated:
    authenticate_user()
    st.stop()
else:
    st.sidebar.markdown("---")
    if hasattr(st.session_state, 'new_user_id') and st.session_state.new_user_id:
        st.sidebar.success(f"**Your ID:** `{st.session_state.new_user_id}`")
        st.sidebar.info("📝 Save this ID to retrieve your data later!")
    elif st.session_state.existing_table:
        st.sidebar.success(f"**Using:** `{st.session_state.existing_table}`")

    st.sidebar.success(f"Logged in as: {st.session_state.user_identifier}")

# Smart Navigation
if st.session_state.user_authenticated:
    # Direct to chatbot if data exists
    if st.session_state.data_stored and st.session_state.existing_table:
        st.success("🎉 Welcome back! Your data is ready for analysis.")

        # Show quick stats
        try:
            user_table_name = list(st.session_state.user_tables.values())[0]
            count_query = f"SELECT COUNT(*) as total_records FROM {user_table_name}"
            count_df = realdatabase.execute_sql_query(count_query)
            if count_df is not None:
                st.info(f"📊 Your database contains **{count_df.iloc[0, 0]:,}** records ready for analysis")
        except:
            pass

        # Show chatbot interface
        st.markdown("---")
        show_chatbot_interface()
    else:
        # Show file upload for new users
        show_file_upload_interface()

# -------------------------
# SIDEBAR - USER MANAGEMENT
# -------------------------
st.sidebar.markdown("---")
st.sidebar.subheader("🔧 User Management")

if st.sidebar.button("🔄 Switch User"):
    for key in defaults.keys():
        st.session_state[key] = defaults[key]
    st.rerun()

if st.sidebar.button("🗑️ Delete My Data"):
    if st.session_state.user_id:
        if realdatabase.delete_all_user_tables(st.session_state.user_id):
            # Clear session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.success("✅ All your data has been deleted!")
            st.rerun()
    else:
        st.error("❌ No user logged in")

# Footer
st.sidebar.markdown("---")
st.sidebar.info(f"""
**User ID:** `{st.session_state.user_id}`  
**Tables:** `{', '.join(st.session_state.user_tables.values()) if st.session_state.user_tables else 'None'}`
""")
