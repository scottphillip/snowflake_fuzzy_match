import streamlit as st
import pandas as pd
import snowflake.connector
import re
import uuid

# --- CONFIG ---
SNOWFLAKE_USER = st.secrets["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = st.secrets["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = st.secrets["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_DATABASE = st.secrets["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA = st.secrets["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_WAREHOUSE = st.secrets["SNOWFLAKE_WAREHOUSE"]

@st.cache_resource
def get_conn():
    try:
        return snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            session_parameters={
                'CLIENT_SESSION_KEEP_ALIVE': True
            }
        )
    except Exception as e:
        st.error(f"❌ Could not connect to Snowflake: {e}")
        st.stop()

ADDRESS_ABBREVIATIONS = {
    r"\bSTREET\b": "ST", r"\bST\.$": "ST", r"\bSAINT\b": "ST",
    r"\bAVENUE\b": "AVE", r"\bAVE\.$": "AVE", r"\bDRIVE\b": "DR",
    r"\bDR\.$": "DR", r"\bCOURT\b": "CT", r"\bCT\.$": "CT",
    r"\bROAD\b": "RD", r"\bRD\.$": "RD", r"\bHIGHWAY\b": "HWY",
    r"\bHWY\.$": "HWY", r"\bNORTH\b": "N", r"\bN\.$": "N",
    r"\bSOUTH\b": "S", r"\bS\.$": "S", r"\bEAST\b": "E",
    r"\bE\.$": "E", r"\bWEST\b": "W", r"\bW\.$": "W"
}

def normalize_address_field(value):
    if pd.isnull(value):
        return ""
    text = str(value).upper()
    for pattern, replacement in ADDRESS_ABBREVIATIONS.items():
        text = re.sub(pattern, replacement, text)
    return re.sub(r'\s+', ' ', text.strip())

st.title("🔍 Affinity Group CRM Matcher")
uploaded = st.file_uploader("Upload file (CSV or Excel)", type=["csv", "xlsx"])

if uploaded:
    df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
    required = ["companyName", "companyAddress", "companyAddress2", "companyCity", "companyState", "companyZipCode"]
    if not all(col in df.columns for col in required):
        st.error(f"Missing required columns: {', '.join(required)}")
    else:
        df["match_key"] = df.apply(lambda r: normalize_address_field(r["companyName"]) + ' ' + str(r["companyZipCode"]), axis=1)
        session_id = str(uuid.uuid4())

        conn = get_conn()
        cur = conn.cursor()

        try:
            insert_sql = """
                INSERT INTO DB_PROD_TRF.SCH_TRF_UTILS.TB_FUZZY_UPLOAD
                (UploadedCompanyName, UploadedAddress, UploadedCity, UploadedState, UploadedZip, match_key, session_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            for _, row in df.iterrows():
                cur.execute(insert_sql, (
                    str(row["companyName"]),
                    str(row["companyAddress"]),
                    str(row["companyCity"]),
                    str(row["companyState"]),
                    str(row["companyZipCode"]),
                    str(row["match_key"]),
                    session_id
                ))

            st.success("✅ Data uploaded. Running match...")

            score_threshold = st.slider("Max allowed name score (lower = better match)", 0, 100, 20)

            query = f"""
                SELECT * FROM DB_PROD_TRF.SCH_TRF_UTILS.VW_FUZZY_MATCH_RESULT
                WHERE session_id = '{session_id}'
            """
            result_df = pd.read_sql(query, conn)
            filtered_df = result_df[result_df["name_score"] <= score_threshold]

            if not filtered_df.empty:
                available_fields = [col for col in filtered_df.columns if col not in ["name_score", "address_score", "UPLOADEDCOMPANYNAME"]]
                selected_fields = st.multiselect("Select CRM fields to include:", available_fields,
                                                 default=["systemId", "companyName", "companyAddress"])

                final_df = filtered_df[["UPLOADEDCOMPANYNAME"] + selected_fields + ["name_score", "address_score"]]
                st.dataframe(final_df)
                st.download_button("Download Matches", final_df.to_csv(index=False), "matched_results.csv", key="download_button")

            else:
                st.warning("⚠️ No matches found under the selected threshold. Try increasing the allowed match score.")

        except Exception as e:
            st.error(f"❌ Matching error: {e}")
