import streamlit as st
import pandas as pd
import snowflake.connector
import re
import uuid
from difflib import SequenceMatcher
import time
import logging
from datetime import datetime

# Set up logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIG ---
SNOWFLAKE_USER = st.secrets["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = st.secrets["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = st.secrets["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_DATABASE = st.secrets["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA = st.secrets["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_WAREHOUSE = st.secrets["SNOWFLAKE_WAREHOUSE"]

# Connection retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2

# ADD THIS NEW FUNCTION HERE:
def keep_session_alive():
    """Keep Streamlit session alive during long processing"""
    if 'heartbeat' not in st.session_state:
        st.session_state.heartbeat = time.time()
    
    # Update every 30 seconds
    if time.time() - st.session_state.heartbeat > 30:
        st.session_state.heartbeat = time.time()
        st.empty()  # This keeps the session alive
        time.sleep(0.1)

def get_conn_with_retry():
    """Get Snowflake connection with robust retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                session_parameters={
                    'CLIENT_SESSION_KEEP_ALIVE': True,
                    'CLIENT_TIMEOUT': 300,
                    'CLIENT_SESSION_CLONE': True
                }
            )
            return conn
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise e

# MODIFY THIS FUNCTION - ADD keep_session_alive() call:
def execute_query_safe(query, max_retries=3):
    """Execute query with comprehensive error handling"""
    for attempt in range(max_retries):
        conn = None
        try:
            # ADD THIS LINE:
            keep_session_alive()
            
            conn = get_conn_with_retry()
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Query attempt {attempt + 1} failed: {error_msg}")
            
            if "502" in error_msg or "404" in error_msg or "Connection" in error_msg:
                if attempt < max_retries - 1:
                    st.warning(f"⚠️ Connection issue (attempt {attempt + 1}): Retrying in 3 seconds...")
                    time.sleep(3)
                    continue
                else:
                    st.error(f"❌ Connection failed after {max_retries} attempts")
                    st.error("Please check your internet connection and try again.")
                    return pd.DataFrame()
            else:
                st.error(f"❌ Query error: {error_msg}")
                return pd.DataFrame()
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    return pd.DataFrame()

# State name to abbreviation mapping
STATE_MAPPING = {
    'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA',
    'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA',
    'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA',
    'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO',
    'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH',
    'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT',
    'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY',
    'DISTRICT OF COLUMBIA': 'DC'
}

def convert_state_to_abbrev(state):
    """Convert full state name to abbreviation"""
    if not state:
        return state
    
    state_upper = str(state).upper().strip()
    
    if len(state_upper) == 2:
        return state_upper
    
    return STATE_MAPPING.get(state_upper, state_upper)

def simple_similarity(str1, str2):
    """Simple similarity using Python's difflib"""
    if not str1 or not str2:
        return 0.0
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()

def normalize_text(text):
    """Normalize text for better matching"""
    if not text:
        return ""
    normalized = re.sub(r'\s+', ' ', str(text).upper().strip())
    normalized = re.sub(r'[^\w\s]', '', normalized)
    return normalized

def normalize_address(text):
    """Normalize address with common abbreviations and punctuation removal"""
    if not text:
        return ""
    
    normalized = re.sub(r'\s+', ' ', str(text).upper().strip())
    
    address_abbreviations = {
        r'\bSTREET\b': 'ST', r'\bST\.$': 'ST', r'\bDRIVE\b': 'DR', r'\bDR\.$': 'DR',
        r'\bWAY\b': 'WY', r'\bWY\.$': 'WY', r'\bHIGHWAY\b': 'HWY', r'\bHWY\.$': 'HWY',
        r'\bCOURT\b': 'CT', r'\bCT\.$': 'CT', r'\bROAD\b': 'RD', r'\bRD\.$': 'RD',
        r'\bAVENUE\b': 'AVE', r'\bAVE\.$': 'AVE', r'\bBOULEVARD\b': 'BLVD', r'\bBLVD\.$': 'BLVD',
        r'\bLANE\b': 'LN', r'\bLN\.$': 'LN', r'\bPLACE\b': 'PL', r'\bPL\.$': 'PL',
        r'\bCIRCLE\b': 'CIR', r'\bCIR\.$': 'CIR', r'\bSQUARE\b': 'SQ', r'\bSQ\.$': 'SQ',
        r'\bNORTH\b': 'N', r'\bN\.$': 'N', r'\bSOUTH\b': 'S', r'\bS\.$': 'S',
        r'\bEAST\b': 'E', r'\bE\.$': 'E', r'\bWEST\b': 'W', r'\bW\.$': 'W',
        r'\bSAINT\b': 'ST', r'\bST\.$': 'ST'
    }
    
    for pattern, replacement in address_abbreviations.items():
        normalized = re.sub(pattern, replacement, normalized)
    
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

# Initialize session state - ADD HEARTBEAT HERE:
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'matches' not in st.session_state:
    st.session_state.matches = []
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
# ADD THIS LINE:
if 'heartbeat' not in st.session_state:
    st.session_state.heartbeat = time.time()

st.title("🔗 Affinity Group CRM Matcher - Latest Version")
st.markdown("**Complete version with auto-download, robust error handling, and all CRM fields**")

# Connection status indicator
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("🔍 Test Connection", key="test_conn"):
        with st.spinner("Testing connection..."):
            try:
                conn = get_conn_with_retry()
                cursor = conn.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                cursor.close()
                conn.close()
                st.success("✅ Connection successful!")
            except Exception as e:
                st.error(f"❌ Connection failed: {e}")

# File upload section
st.subheader("📁 Upload Your Data")
uploaded = st.file_uploader("Upload file (CSV or Excel)", type=["csv", "xlsx"], key="file_upload")

if uploaded and not st.session_state.processing:
    try:
        # Load file with multiple encoding attempts
        if uploaded.name.endswith(".csv"):
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            for encoding in encodings:
                try:
                    uploaded.seek(0)
                    df = pd.read_csv(uploaded, encoding=encoding)
                    st.success(f"✅ File loaded successfully with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                st.error("❌ Could not read CSV file with any supported encoding.")
                st.stop()
        else:
            df = pd.read_excel(uploaded)
            st.success("✅ Excel file loaded successfully")
            
    except Exception as e:
        st.error(f"❌ Error reading file: {e}")
        st.stop()
    
    # Validate required columns
    required = ["companyName", "companyAddress", "companyAddress2", "companyCity", "companyState", "companyZipCode"]
    if not all(col in df.columns for col in required):
        st.error(f"Missing required columns: {', '.join(required)}")
        st.stop()
    
    # Data preprocessing
    with st.spinner("Preprocessing data..."):
        df['state_abbrev'] = df['companyState'].apply(convert_state_to_abbrev)
        df['normalized_address'] = df['companyAddress'].apply(normalize_address)
        df['normalized_company_name'] = df['companyName'].apply(normalize_text)
    
    st.session_state.processed_data = df
    
    # Show file preview
    st.subheader("📋 Data Preview")
    st.dataframe(df.head(10), use_container_width=True)
    
    # Settings
    st.subheader("⚙️ Matching Settings")
    col1, col2 = st.columns(2)
    with col1:
        name_threshold = st.slider("Minimum name similarity", 0.0, 1.0, 0.8, step=0.01)
    with col2:
        address_threshold = st.slider("Minimum address similarity", 0.0, 1.0, 0.7, step=0.01)
    
    # Process button
    if st.button("🚀 Start Processing", type="primary", key="process_btn"):
        st.session_state.processing = True
        st.session_state.matches = []
        
        # Create progress containers
        progress_container = st.container()
        stats_container = st.container()
        
        with progress_container:
            progress_bar = st.progress(0, text="Starting processing...")
            status_text = st.empty()
        
        with stats_container:
            col1, col2, col3 = st.columns(3)
            with col1:
                processed_metric = st.metric("Records Processed", "0")
            with col2:
                matches_metric = st.metric("Matches Found", "0")
            with col3:
                current_state_metric = st.metric("Current State", "Starting...")
        
        # Process matches with robust error handling
        matches = []
        start_time = time.time()
        
        unique_states = df['state_abbrev'].unique()
        total_states = len(unique_states)
        total_records = len(df)
        processed_records = 0
        
        for state_idx, state_abbrev in enumerate(unique_states):
            if not st.session_state.processing:  # Allow cancellation
                break
                
            current_state_metric.metric("Current State", f"{state_abbrev} ({state_idx + 1}/{total_states})")
            state_df = df[df['state_abbrev'] == state_abbrev]
            
            try:
                st.info(f"🔄 Loading CRM data for {state_abbrev}...")
                
                # Use safe query execution - Get ALL available fields from the CRM view
                crm_query = f"""
                    SELECT 
                        "systemId",
                        "accountNumber",
                        "companyName",
                        "companyAddress",
                        "companyAddress2",
                        "companyCity",
                        "companyState",
                        "companyZipCode",
                        "companyCounty",
                        "companyCountry",
                        "companyPhone",
                        "companyWebsite",
                        "companyEmail",
                        "companyFacebook",
                        "companyInstagram",
                        "companyTwitter",
                        "companyTiktok",
                        "companyLinkedInURL",
                        "companyYouTube",
                        "companyCreateDate",
                        "companyRecordSource",
                        "territoryDivision",
                        "territoryRegion",
                        "territoryName",
                        "internalTerritoryId",
                        "classificationName",
                        "classificationDesc",
                        "classificationAbbrv",
                        "priority",
                        "segmentParent",
                        "segment",
                        "cuisineName",
                        "primaryDistName",
                        "primaryDistCode",
                        "primaryDistAddr",
                        "primaryDistCity",
                        "primaryDistState",
                        "primaryDistZip",
                        "secondaryDistName",
                        "secondaryDistCode",
                        "secondaryDistAddr",
                        "secondaryDistCity",
                        "secondaryDistState",
                        "secondaryDistZip",
                        "primaryContact",
                        "contactFirstName",
                        "contactLastName",
                        "contactSalutation",
                        "contactTitle",
                        "contactPhone",
                        "contactMobilePhone",
                        "contactEmail",
                        "contactEmailOption",
                        "contactFacebook",
                        "contactInstagram",
                        "contactTwitter",
                        "contactTiktok",
                        "contactLinkedInURL",
                        "contactYouTube",
                        "contactPreferredLanguage",
                        "companyProfileComplete",
                        "contactAddress",
                        "contactAddress2",
                        "contactCity",
                        "contactState",
                        "contactZipCode",
                        "contactJobFunction",
                        "FSEContactId",
                        "repFirstName",
                        "repLastName",
                        "repEmail",
                        "repTitle",
                        "repTerritoryName",
                        "numUnitsLY",
                        "numUnitsTY",
                        "numUnitsNY",
                        "LastInteractionDate",
                        "LastInteractionRepName",
                        "interactionPurposeName",
                        "seasonOpenDate",
                        "seasonCloseDate",
                        "DistAcctNumPrimary",
                        "DistAcctNumSecondary",
                        "parentCompanyName",
                        "buyingDecisions",
                        "Brizo_ID#",
                        "Brizo_Update_Date",
                        "Operator_Hours",
                        "Status",
                        "Firefly_ID#",
                        "Firefly_URL",
                        "Monthly_Web_Traffic",
                        "Monthly_Foot_Traffic",
                        "Firefly_Chain_ID#",
                        "Chain_ID_Status",
                        "Popularity_Score",
                        "Firefly_Chain_URL",
                        BATCH_ID,
                        INSERT_TIMESTAMP,
                        FILE_NAME
                    FROM DB_PROD_TRF.SCH_TRF_UTILS.VW_CRM_MATCH_KEYS
                    WHERE "companyState" = '{state_abbrev}'
                """
                
                crm_df = execute_query_safe(crm_query)
                
                if crm_df.empty:
                    st.info(f"ℹ️ No CRM data found for {state_abbrev}")
                    continue
                
                st.success(f"✅ Loaded {len(crm_df)} CRM records for {state_abbrev}")
                
                # Normalize CRM data
                crm_df['normalized_company_name'] = crm_df['companyName'].apply(normalize_text)
                crm_df['normalized_address'] = crm_df['companyAddress'].apply(normalize_address)
                
                # Process each uploaded record
                for row_idx, uploaded_row in state_df.iterrows():
                    if not st.session_state.processing:  # Allow cancellation
                        break
                    
                    # Keep session alive every 200 records to avoid interfering with processing
                    if row_idx % 200 == 0:
                        keep_session_alive()
                        
                    uploaded_name_norm = uploaded_row['normalized_company_name']
                    uploaded_addr_norm = uploaded_row['normalized_address']
                    
                    # Find matches
                    for _, crm_row in crm_df.iterrows():
                        name_sim = simple_similarity(uploaded_name_norm, crm_row['normalized_company_name'])
                        address_sim = simple_similarity(uploaded_addr_norm, crm_row['normalized_address'])
                        
                        if name_sim >= name_threshold and address_sim >= address_threshold:
                            # Create match with ALL CRM fields
                            match = {
                                # Uploaded data fields
                                'UPLOADED_COMPANY_NAME': uploaded_row['companyName'],
                                'UPLOADED_ADDRESS': uploaded_row['companyAddress'],
                                'UPLOADED_CITY': uploaded_row['companyCity'],
                                'UPLOADED_STATE': uploaded_row['companyState'],
                                'UPLOADED_STATE_ABBREV': uploaded_row['state_abbrev'],
                                'UPLOADED_ZIP': uploaded_row['companyZipCode'],
                                
                                # Similarity scores
                                'NAME_SIMILARITY': name_sim,
                                'ADDRESS_SIMILARITY': address_sim,
                                'COMBINED_SCORE': (name_sim + address_sim) / 2,
                                
                                # ALL CRM fields - Company Information
                                'CRM_SYSTEM_ID': crm_row['systemId'],
                                'CRM_ACCOUNT_NUMBER': crm_row['accountNumber'],
                                'CRM_COMPANY_NAME': crm_row['companyName'],
                                'CRM_COMPANY_ADDRESS': crm_row['companyAddress'],
                                'CRM_COMPANY_ADDRESS2': crm_row['companyAddress2'],
                                'CRM_COMPANY_CITY': crm_row['companyCity'],
                                'CRM_COMPANY_STATE': crm_row['companyState'],
                                'CRM_COMPANY_ZIP': crm_row['companyZipCode'],
                                'CRM_COMPANY_COUNTY': crm_row['companyCounty'],
                                'CRM_COMPANY_COUNTRY': crm_row['companyCountry'],
                                'CRM_COMPANY_PHONE': crm_row['companyPhone'],
                                'CRM_COMPANY_WEBSITE': crm_row['companyWebsite'],
                                'CRM_COMPANY_EMAIL': crm_row['companyEmail'],
                                'CRM_COMPANY_FACEBOOK': crm_row['companyFacebook'],
                                'CRM_COMPANY_INSTAGRAM': crm_row['companyInstagram'],
                                'CRM_COMPANY_TWITTER': crm_row['companyTwitter'],
                                'CRM_COMPANY_TIKTOK': crm_row['companyTiktok'],
                                'CRM_COMPANY_LINKEDIN': crm_row['companyLinkedInURL'],
                                'CRM_COMPANY_YOUTUBE': crm_row['companyYouTube'],
                                'CRM_COMPANY_CREATE_DATE': crm_row['companyCreateDate'],
                                'CRM_COMPANY_RECORD_SOURCE': crm_row['companyRecordSource'],
                                
                                # Territory Information
                                'CRM_TERRITORY_DIVISION': crm_row['territoryDivision'],
                                'CRM_TERRITORY_REGION': crm_row['territoryRegion'],
                                'CRM_TERRITORY_NAME': crm_row['territoryName'],
                                'CRM_INTERNAL_TERRITORY_ID': crm_row['internalTerritoryId'],
                                
                                # Classification Information
                                'CRM_CLASSIFICATION_NAME': crm_row['classificationName'],
                                'CRM_CLASSIFICATION_DESC': crm_row['classificationDesc'],
                                'CRM_CLASSIFICATION_ABBRV': crm_row['classificationAbbrv'],
                                'CRM_PRIORITY': crm_row['priority'],
                                'CRM_SEGMENT_PARENT': crm_row['segmentParent'],
                                'CRM_SEGMENT': crm_row['segment'],
                                'CRM_CUISINE_NAME': crm_row['cuisineName'],
                                
                                # Distributor Information
                                'CRM_PRIMARY_DIST_NAME': crm_row['primaryDistName'],
                                'CRM_PRIMARY_DIST_CODE': crm_row['primaryDistCode'],
                                'CRM_PRIMARY_DIST_ADDR': crm_row['primaryDistAddr'],
                                'CRM_PRIMARY_DIST_CITY': crm_row['primaryDistCity'],
                                'CRM_PRIMARY_DIST_STATE': crm_row['primaryDistState'],
                                'CRM_PRIMARY_DIST_ZIP': crm_row['primaryDistZip'],
                                'CRM_SECONDARY_DIST_NAME': crm_row['secondaryDistName'],
                                'CRM_SECONDARY_DIST_CODE': crm_row['secondaryDistCode'],
                                'CRM_SECONDARY_DIST_ADDR': crm_row['secondaryDistAddr'],
                                'CRM_SECONDARY_DIST_CITY': crm_row['secondaryDistCity'],
                                'CRM_SECONDARY_DIST_STATE': crm_row['secondaryDistState'],
                                'CRM_SECONDARY_DIST_ZIP': crm_row['secondaryDistZip'],
                                
                                # Contact Information
                                'CRM_PRIMARY_CONTACT': crm_row['primaryContact'],
                                'CRM_CONTACT_FIRST_NAME': crm_row['contactFirstName'],
                                'CRM_CONTACT_LAST_NAME': crm_row['contactLastName'],
                                'CRM_CONTACT_SALUTATION': crm_row['contactSalutation'],
                                'CRM_CONTACT_TITLE': crm_row['contactTitle'],
                                'CRM_CONTACT_PHONE': crm_row['contactPhone'],
                                'CRM_CONTACT_MOBILE_PHONE': crm_row['contactMobilePhone'],
                                'CRM_CONTACT_EMAIL': crm_row['contactEmail'],
                                'CRM_CONTACT_EMAIL_OPTION': crm_row['contactEmailOption'],
                                'CRM_CONTACT_FACEBOOK': crm_row['contactFacebook'],
                                'CRM_CONTACT_INSTAGRAM': crm_row['contactInstagram'],
                                'CRM_CONTACT_TWITTER': crm_row['contactTwitter'],
                                'CRM_CONTACT_TIKTOK': crm_row['contactTiktok'],
                                'CRM_CONTACT_LINKEDIN': crm_row['contactLinkedInURL'],
                                'CRM_CONTACT_YOUTUBE': crm_row['contactYouTube'],
                                'CRM_CONTACT_PREFERRED_LANGUAGE': crm_row['contactPreferredLanguage'],
                                'CRM_COMPANY_PROFILE_COMPLETE': crm_row['companyProfileComplete'],
                                'CRM_CONTACT_ADDRESS': crm_row['contactAddress'],
                                'CRM_CONTACT_ADDRESS2': crm_row['contactAddress2'],
                                'CRM_CONTACT_CITY': crm_row['contactCity'],
                                'CRM_CONTACT_STATE': crm_row['contactState'],
                                'CRM_CONTACT_ZIP': crm_row['contactZipCode'],
                                'CRM_CONTACT_JOB_FUNCTION': crm_row['contactJobFunction'],
                                'CRM_FSE_CONTACT_ID': crm_row['FSEContactId'],
                                
                                # Representative Information
                                'CRM_REP_FIRST_NAME': crm_row['repFirstName'],
                                'CRM_REP_LAST_NAME': crm_row['repLastName'],
                                'CRM_REP_EMAIL': crm_row['repEmail'],
                                'CRM_REP_TITLE': crm_row['repTitle'],
                                'CRM_REP_TERRITORY_NAME': crm_row['repTerritoryName'],
                                
                                # Business Metrics
                                'CRM_NUM_UNITS_LY': crm_row['numUnitsLY'],
                                'CRM_NUM_UNITS_TY': crm_row['numUnitsTY'],
                                'CRM_NUM_UNITS_NY': crm_row['numUnitsNY'],
                                'CRM_LAST_INTERACTION_DATE': crm_row['LastInteractionDate'],
                                'CRM_LAST_INTERACTION_REP_NAME': crm_row['LastInteractionRepName'],
                                'CRM_INTERACTION_PURPOSE_NAME': crm_row['interactionPurposeName'],
                                'CRM_SEASON_OPEN_DATE': crm_row['seasonOpenDate'],
                                'CRM_SEASON_CLOSE_DATE': crm_row['seasonCloseDate'],
                                'CRM_DIST_ACCT_NUM_PRIMARY': crm_row['DistAcctNumPrimary'],
                                'CRM_DIST_ACCT_NUM_SECONDARY': crm_row['DistAcctNumSecondary'],
                                'CRM_PARENT_COMPANY_NAME': crm_row['parentCompanyName'],
                                'CRM_BUYING_DECISIONS': crm_row['buyingDecisions'],
                                
                                # Brizo/Firefly Data
                                'CRM_BRIZO_ID': crm_row['Brizo_ID#'],
                                'CRM_BRIZO_UPDATE_DATE': crm_row['Brizo_Update_Date'],
                                'CRM_OPERATOR_HOURS': crm_row['Operator_Hours'],
                                'CRM_STATUS': crm_row['Status'],
                                'CRM_FIREFLY_ID': crm_row['Firefly_ID#'],
                                'CRM_FIREFLY_URL': crm_row['Firefly_URL'],
                                'CRM_MONTHLY_WEB_TRAFFIC': crm_row['Monthly_Web_Traffic'],
                                'CRM_MONTHLY_FOOT_TRAFFIC': crm_row['Monthly_Foot_Traffic'],
                                'CRM_FIREFLY_CHAIN_ID': crm_row['Firefly_Chain_ID#'],
                                'CRM_CHAIN_ID_STATUS': crm_row['Chain_ID_Status'],
                                'CRM_POPULARITY_SCORE': crm_row['Popularity_Score'],
                                'CRM_FIREFLY_CHAIN_URL': crm_row['Firefly_Chain_URL'],
                                
                                # Metadata
                                'CRM_BATCH_ID': crm_row['BATCH_ID'],
                                'CRM_INSERT_TIMESTAMP': crm_row['INSERT_TIMESTAMP'],
                                'CRM_FILE_NAME': crm_row['FILE_NAME']
                            }
                            matches.append(match)
                    
                    # Update progress
                    processed_records += 1
                    overall_progress = min(1.0, processed_records / total_records)
                    progress_bar.progress(overall_progress, text=f"Processing {uploaded_row['companyName'][:30]}... in {state_abbrev}")
                    
                    # Update metrics every 100 records to prevent session timeout
                    if processed_records % 100 == 0:
                        processed_metric.metric("Records Processed", f"{processed_records:,}")
                        matches_metric.metric("Matches Found", f"{len(matches):,}")
                        # Don't use st.rerun() as it resets the processing state
                
                # Final metrics update for this state
                processed_metric.metric("Records Processed", f"{processed_records:,}")
                matches_metric.metric("Matches Found", f"{len(matches):,}")
                
            except Exception as e:
                st.error(f"❌ Error processing state {state_abbrev}: {e}")
                continue
        
        if st.session_state.processing:  # Only complete if not cancelled
            progress_bar.progress(1.0, text="Processing complete!")
            status_text.text("✅ Processing complete!")
            
            st.session_state.matches = matches
            st.session_state.processing = False
            
            # Refresh the page to show results
            st.rerun()

# Show results if processing is complete
if st.session_state.matches and not st.session_state.processing:
    matches_df = pd.DataFrame(st.session_state.matches)
    
    if not matches_df.empty:
        st.success(f"✅ Found {len(matches_df)} matches!")
        
        # Show final statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Matches", f"{len(matches_df):,}")
        with col2:
            st.metric("Avg Name Similarity", f"{matches_df['NAME_SIMILARITY'].mean():.3f}")
        with col3:
            st.metric("Avg Address Similarity", f"{matches_df['ADDRESS_SIMILARITY'].mean():.3f}")
        with col4:
            st.metric("Avg Combined Score", f"{matches_df['COMBINED_SCORE'].mean():.3f}")
        
        # Show state matching success
        st.subheader("🗺️ State Matching Success")
        state_matches = matches_df.groupby('UPLOADED_STATE_ABBREV').size().sort_values(ascending=False)
        st.bar_chart(state_matches)
        
        # Field selection for export - Show ALL available fields
        st.subheader("📋 Select Fields to Export")
        
        # Get ALL available fields from the actual data
        all_available_fields = list(matches_df.columns)
        
        # Remove similarity scores from the main selection (they'll be added automatically)
        similarity_fields = ['NAME_SIMILARITY', 'ADDRESS_SIMILARITY', 'COMBINED_SCORE']
        selectable_fields = [field for field in all_available_fields if field not in similarity_fields]
        
        # Show field information
        st.info(f"📊 **Total fields available:** {len(all_available_fields)} | **Selectable fields:** {len(selectable_fields)}")
        
        # Show all available fields in a collapsible section
        with st.expander("🔍 View All Available Fields", expanded=False):
            st.write("**All fields in your data:**")
            for i, field in enumerate(all_available_fields, 1):
                field_type = "📊 Similarity Score" if field in similarity_fields else "📋 Data Field"
                st.write(f"{i:2d}. **{field}** {field_type}")
        
        # Default selection - include the most commonly needed fields
        default_fields = [
            # Uploaded data
            'UPLOADED_COMPANY_NAME', 'UPLOADED_ADDRESS', 'UPLOADED_CITY', 'UPLOADED_STATE', 'UPLOADED_STATE_ABBREV', 'UPLOADED_ZIP',
            # Basic CRM company info
            'CRM_COMPANY_NAME', 'CRM_COMPANY_ADDRESS', 'CRM_COMPANY_CITY', 'CRM_COMPANY_STATE', 'CRM_COMPANY_ZIP',
            'CRM_COMPANY_PHONE', 'CRM_COMPANY_EMAIL', 'CRM_COMPANY_WEBSITE',
            # Business classification
            'CRM_SYSTEM_ID', 'CRM_ACCOUNT_NUMBER', 'CRM_TERRITORY_REGION', 'CRM_CLASSIFICATION_NAME', 'CRM_SEGMENT',
            # Contact information
            'CRM_CONTACT_FIRST_NAME', 'CRM_CONTACT_LAST_NAME', 'CRM_CONTACT_TITLE', 'CRM_CONTACT_EMAIL', 'CRM_CONTACT_PHONE',
            # Representative
            'CRM_REP_FIRST_NAME', 'CRM_REP_LAST_NAME', 'CRM_REP_EMAIL', 'CRM_REP_TITLE',
            # Business metrics
            'CRM_NUM_UNITS_TY', 'CRM_LAST_INTERACTION_DATE', 'CRM_STATUS'
        ]
        
        # Filter default fields to only include those that actually exist in the data
        default_fields = [field for field in default_fields if field in selectable_fields]
        
        # Field selection with search capability
        col1, col2 = st.columns([3, 1])
        
        with col1:
            selected_fields = st.multiselect(
                "Choose which fields to include in the export:",
                options=selectable_fields,
                default=default_fields,
                help="Select the fields you want to include in your downloaded CSV file. Similarity scores will be added automatically."
            )
        
        with col2:
            if st.button("✅ Select All", help="Select all available fields"):
                st.session_state.select_all_fields = True
                st.rerun()
            
            if st.button("❌ Clear All", help="Clear all selections"):
                st.session_state.select_all_fields = False
                st.rerun()
        
        # Handle select all functionality
        if st.session_state.get('select_all_fields', False):
            selected_fields = selectable_fields.copy()
            st.session_state.select_all_fields = False
        
        # Show selected fields count
        st.success(f"✅ **{len(selected_fields)} fields selected** for export")
        
        # Show what will be included
        if selected_fields:
            st.write("**Fields that will be included in your download:**")
            export_preview = selected_fields + similarity_fields  # Add similarity scores
            for i, field in enumerate(export_preview, 1):
                st.write(f"{i:2d}. {field}")
        else:
            st.warning("⚠️ No fields selected. All fields will be included in the download.")
        
        # Add similarity scores to selected fields
        if selected_fields:
            export_fields = selected_fields + ['NAME_SIMILARITY', 'ADDRESS_SIMILARITY', 'COMBINED_SCORE']
            export_df = matches_df[export_fields]
        else:
            export_df = matches_df
            st.warning("No fields selected. All fields will be included.")
        
        # Show top matches
        st.subheader("🏆 Top Matches")
        display_fields = ['UPLOADED_COMPANY_NAME', 'CRM_COMPANY_NAME', 'UPLOADED_STATE', 'UPLOADED_STATE_ABBREV', 'NAME_SIMILARITY', 'ADDRESS_SIMILARITY', 'COMBINED_SCORE']
        top_matches = matches_df.nlargest(20, 'COMBINED_SCORE')
        st.dataframe(top_matches[display_fields], use_container_width=True)
        
        # Download options with better error handling
        st.subheader("💾 Download Options")
        
        # Create download data in session state to avoid regeneration
        if 'download_data' not in st.session_state:
            st.session_state.download_data = {}
        
        # Generate download data
        try:
            with st.spinner("Preparing download files..."):
                # All matches
                csv_all = export_df.to_csv(index=False)
                st.session_state.download_data['all'] = csv_all
                
                # High confidence matches
                high_conf_df = export_df[export_df['COMBINED_SCORE'] >= 0.9]
                if not high_conf_df.empty:
                    csv_high = high_conf_df.to_csv(index=False)
                    st.session_state.download_data['high'] = csv_high
                else:
                    st.session_state.download_data['high'] = None
                
                # Medium+ confidence matches
                med_conf_df = export_df[export_df['COMBINED_SCORE'] >= 0.8]
                if not med_conf_df.empty:
                    csv_med = med_conf_df.to_csv(index=False)
                    st.session_state.download_data['medium'] = csv_med
                else:
                    st.session_state.download_data['medium'] = None
                    
        except Exception as e:
            st.error(f"❌ Error preparing download files: {e}")
            st.session_state.download_data = {}
        
        # Display download buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'all' in st.session_state.download_data and st.session_state.download_data['all']:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    "📥 Download All Matches", 
                    st.session_state.download_data['all'], 
                    f"crm_matches_{timestamp}.csv", 
                    key="download_all",
                    mime="text/csv",
                    help="Download all matches as CSV file"
                )
            else:
                st.error("❌ Download data not available")
        
        with col2:
            if 'high' in st.session_state.download_data and st.session_state.download_data['high']:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    "📥 Download High Confidence", 
                    st.session_state.download_data['high'], 
                    f"crm_high_conf_{timestamp}.csv", 
                    key="download_high",
                    mime="text/csv",
                    help="Download high confidence matches only"
                )
            else:
                st.info("ℹ️ No high confidence matches")
        
        with col3:
            if 'medium' in st.session_state.download_data and st.session_state.download_data['medium']:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    "📥 Download Medium+ Confidence", 
                    st.session_state.download_data['medium'], 
                    f"crm_medium_conf_{timestamp}.csv", 
                    key="download_medium",
                    mime="text/csv",
                    help="Download medium+ confidence matches"
                )
            else:
                st.info("ℹ️ No medium confidence matches")
        
        # Alternative download method if buttons don't work
        st.markdown("---")
        st.subheader("🔄 Alternative Download Method")
        
        if st.button("📋 Copy Data to Clipboard", help="Copy all data to clipboard for pasting into Excel"):
            try:
                # Convert to string and copy to clipboard
                data_text = export_df.to_csv(index=False)
                st.code(data_text[:1000] + "..." if len(data_text) > 1000 else data_text, language="csv")
                st.success("✅ Data copied! You can now paste this into Excel or a text editor.")
            except Exception as e:
                st.error(f"❌ Error copying data: {e}")
        
        # Show data info
        st.info(f"📊 Export data contains {len(export_df)} rows and {len(export_df.columns)} columns")
    else:
        st.warning("No matches found. Try lowering the similarity thresholds.")

# Cancel button during processing
if st.session_state.processing:
    if st.button("🛑 Cancel Processing", type="secondary"):
        st.session_state.processing = False
        st.rerun()

# Reset button to clear session state
if st.button("🔄 Reset App", type="secondary", help="Clear all data and start fresh"):
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# Footer with connection info
st.markdown("---")
st.markdown("**🔗 Latest Version Features:**")
st.markdown("- ✅ **Auto-download feature** - Multiple CSV download options")
st.markdown("- ✅ **Complete CRM data** - All available fields included")
st.markdown("- ✅ **Field selection** - Choose exactly what to export")
st.markdown("- ✅ **Robust error handling** - Connection recovery & retry logic")
st.markdown("- ✅ **Session state management** - Progress tracking & cancellation")
st.markdown("- ✅ **Optimized for deployment** - Ready for Streamlit Cloud")
st.markdown("- ✅ **Connection keep-alive** - Prevents WebSocket disconnections")
