import streamlit as st
import pandas as pd
import snowflake.connector
import re

# --- CONFIG ---
SNOWFLAKE_USER = st.secrets["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = st.secrets["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = st.secrets["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_DATABASE = st.secrets["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA = st.secrets["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_WAREHOUSE = st.secrets["SNOWFLAKE_WAREHOUSE"]

# Connect to Snowflake
@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

# Normalize addresses
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
st.markdown("Upload a customer list to match against our CRM system.")

uploaded = st.file_uploader("Upload file (CSV or Excel)", type=["csv", "xlsx"])

if uploaded:
    df = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
    required = ["companyName", "companyAddress", "companyAddress2", "companyCity", "companyState", "companyZipCode"]
    if not all(col in df.columns for col in required):
        st.error(f"Missing required columns: {', '.join(required)}")
    else:
        df["match_key"] = df.apply(lambda r: normalize_address_field(r["companyName"]) + ' ' +
                                          normalize_address_field(r["companyAddress"]) + ' ' +
                                          normalize_address_field(r["companyAddress2"]) + ' ' +
                                          normalize_address_field(r["companyCity"]) + ' ' +
                                          normalize_address_field(r["companyState"]) + ' ' +
                                          str(r["companyZipCode"]), axis=1)

        conn = get_conn()
        cur = conn.cursor()

        try:
            cur.execute("TRUNCATE TABLE DB_PROD_TRF.SCH_TRF_UTILS.TB_FUZZY_UPLOAD")

            insert_sql = """
                INSERT INTO DB_PROD_TRF.SCH_TRF_UTILS.TB_FUZZY_UPLOAD
                (UploadedCompanyName, UploadedAddress, UploadedCity, UploadedState, UploadedZip, match_key)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            for _, row in df.iterrows():
                try:
                    cur.execute(insert_sql, (
                        str(row["companyName"]),
                        str(row["companyAddress"]),
                        str(row["companyCity"]),
                        str(row["companyState"]),
                        str(row["companyZipCode"]),
                        str(row["match_key"])
                    ))
                except Exception as insert_error:
                    st.warning(f"Failed to insert row: {insert_error}")

            st.success("✅ Data uploaded. Running match...")

            result_df = pd.read_sql("SELECT * FROM DB_PROD_TRF.SCH_TRF_UTILS.VW_FUZZY_MATCH_RESULT", conn)

            # Dynamically detect all available fields from CRM view (excluding the upload and score metadata)
            available_fields = [col for col in result_df.columns if col not in ["match_score", "UploadedCompanyName"]]

            selected_fields = st.multiselect(
                "Select CRM fields to include in result:",
                available_fields,
                default=["systemId", "companyName", "companyAddress"]
            )

            # Safely subset dataframe based on user-selected fields
            final_df = result_df[["UploadedCompanyName"] + selected_fields + ["match_score"]]
            st.dataframe(final_df)

            # Download option
            st.download_button("Download Matches", final_df.to_csv(index=False), "matched_results.csv")

            st.dataframe(result_df)

            st.download_button("Download Matches", result_df.to_csv(index=False), "matched_results.csv")

        except Exception as e:
            st.error(f"❌ Matching error: {e}")
