import sqlite3
import pandas as pd
import os
import re
import unicodedata
import numpy as np
from datetime import datetime, time, timedelta

# Define the SQLite database file name
DATABASE_NAME = 'foodstory_bills.db'

def normalize_column_name(col_name: str) -> str:
    """
    Aggressively normalizes column names for robust matching:
    1. Converts to NFKC Unicode form (handles composite characters, e.g., Thai characters with combining marks).
    2. Removes all whitespace (including tabs, newlines, non-breaking spaces).
    3. Removes all non-alphanumeric characters, except for the dot in "INV. No" and "Custom Payment Ref." patterns.
    4. Converts to lowercase.
    """
    normalized = unicodedata.normalize('NFKC', col_name)
    normalized = re.sub(r'\s+', '', normalized)

    lower_col_name = col_name.lower()
    if "inv.no" in lower_col_name or "custom payment ref." in lower_col_name or "custompaymentref." in lower_col_name:
        normalized = re.sub(r'[^\w.]', '', normalized)
    else:
        normalized = re.sub(r'[^\w]', '', normalized)

    return normalized.lower()

# --- Column Mappings ---
BILLS_COLUMN_MAPPING = {
    'Payment Date': 'payment_date',
    'Payment Time': 'payment_time',
    'Time': 'time_col', # Keeping this, though not directly used in analysis
    'Receipt Number': 'receipt_number',
    'POS ID': 'pos_id',
    'INV. No': 'inv_no',
    'Summary Price': 'summary_price',
    'Discount By Item': 'discount_by_item',
    'Subtotal Bill Discount': 'subtotal_bill_discount',
    'Subtotal Summary Price - Discount By Item': 'subtotal_summary_price_minus_discount_by_item',
    'Service charge': 'service_charge',
    'Non VAT': 'non_vat',
    'Ex. VAT': 'ex_vat',
    'Before Vat Subtotal + Service charge': 'before_vat_subtotal_plus_service_charge',
    'VAT': 'vat',
    'Voucher Amount มูลค่า Voucher มีโอกาสที่จะมากกว่ายอดรวมทั้งบิล ส่วนลดที่ใช้ได้สูงสุดจึงเป็นยอดรวมของบิล': 'voucher_amount_desc',
    'Voucher Discount': 'voucher_discount',
    'Rouding amount': 'rounding_amt',
    'Delivery Fee': 'delivery_fee',
    'Total (Before Vat + VAT + Rouding amount) - Non-VAT amount': 'total_final_bill',
    'Tips': 'tips',
    'Refund': 'refund',
    'Order Type': 'order_type',
    'Drawer ID': 'drawer_id',
    'Payment Type': 'payment_type',
    'Custom Payment Ref.': 'custom_payment_ref',
    'Channel': 'channel',
    'Table': 'table_num',
    'Seat Amount': 'seat_amount',
    'Customer Name': 'customer_name',
    'Remark': 'remark',
    'Promotion Code': 'promotion_code',
    'Bill open by': 'bill_open_by',
    'Bill close by': 'bill_close_by',
    'Branch': 'branch'
}

DETAIL_BILLS_COLUMN_MAPPING = {
    'Payment Date': 'payment_date',
    'Payment Time': 'payment_time',
    'Receipt Number': 'receipt_number',
    'INV. No': 'inv_no',
    'Drawer ID': 'drawer_id',
    'Menu Code': 'menu_code',
    'Menu Name': 'menu_name',
    'Order Type': 'order_type',
    'Quantity': 'quantity',
    'Price per unit': 'price_per_unit',
    'Summary Price': 'summary_price',
    'Discount By Item': 'discount_by_item',
    'Discount By Item Percent': 'discount_by_item_percent',
    'Discounted Price': 'discounted_price',
    'VATable type': 'vatable_type',
    'Channel': 'channel',
    'Table': 'table_num',
    'Customer Name': 'customer_name',
    'Phone Number': 'phone_number',
    'Payment Type': 'payment_type',
    'Custom Payment Ref.': 'custom_payment_ref',
    'Remark': 'remark',
    'Group': 'group_col',
    'Category': 'category',
    'Bill open by': 'bill_open_by',
    'Bill close by': 'bill_close_by',
    'Branch': 'branch',
    'Voucher Amount มูลค่า Voucher มีโอกาสที่จะมากกว่ายอดรวมทั้งบิล ส่วนลดที่ใช้ได้สูงสุดจึงเป็นยอดรวมของบิล': 'voucher_amount_desc',
    'Voucher Discount': 'voucher_discount',
    'Rouding amount': 'rounding_amt',
}

def get_sql_type(column_name: str) -> str:
    """
    Determines the SQL data type based on the standardized column name.
    """
    numeric_cols = [
        'summary_price', 'discount_by_item', 'subtotal_bill_discount',
        'subtotal_summary_price_minus_discount_by_item', 'service_charge',
        'non_vat', 'ex_vat', 'before_vat_subtotal_plus_service_charge',
        'vat', 'voucher_amount_desc', 'voucher_discount', 'rounding_amt',
        'delivery_fee', 'total_final_bill', 'tips', 'quantity', 'price_per_unit',
        'discount_by_item_percent', 'discounted_price', 'refund'
    ]
    integer_cols = ['seat_amount']

    if column_name in numeric_cols:
        return "REAL"
    elif column_name in integer_cols:
        return "INTEGER"
    # Specific handling for payment_date and payment_time if stored as TEXT
    elif column_name in ['payment_date', 'payment_time']:
        return "TEXT"
    else:
        return "TEXT"

def create_table_from_mapping(table_name: str, column_mapping: dict):
    """
    Generic function to create a table based on a given column mapping.
    Uses CREATE TABLE IF NOT EXISTS.
    """
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        columns_sql_parts = []
        for db_col in column_mapping.values():
            col_type = get_sql_type(db_col)
            # Define NOT NULL constraints for critical columns, providing default values for robust insertion
            if db_col in ['receipt_number', 'menu_name']:
                columns_sql_parts.append(f'"{db_col}" TEXT NOT NULL DEFAULT \'\'')
            elif db_col in ['total_final_bill', 'quantity', 'summary_price', 'price_per_unit', 'discounted_price']:
                columns_sql_parts.append(f'"{db_col}" REAL NOT NULL DEFAULT 0.0')
            elif db_col == 'seat_amount':
                columns_sql_parts.append(f'"{db_col}" INTEGER NOT NULL DEFAULT 0')
            # For voucher_amount_desc and voucher_discount, if they are numeric, set default to 0.0
            elif db_col in ['voucher_amount_desc', 'voucher_discount'] and col_type == "REAL":
                columns_sql_parts.append(f'"{db_col}" REAL NOT NULL DEFAULT 0.0')
            else:
                columns_sql_parts.append(f'"{db_col}" {col_type}')

        columns_sql = ", ".join(columns_sql_parts)

        create_query = f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns_sql},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        '''
        cursor.execute(create_query)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating '{table_name}' table: {e}")
    finally:
        if conn:
            conn.close()

def create_bills_table():
    create_table_from_mapping('bills', BILLS_COLUMN_MAPPING)

def create_bill_details_table():
    create_table_from_mapping('bill_details', DETAIL_BILLS_COLUMN_MAPPING)

def format_time_string_for_storage(t_str):
    """
    Converts various time string formats (HH:MM:SS, HH:MM, X min, X hour Y min)
    to a consistent HH:MM:SS format for storage.
    """
    if pd.isna(t_str) or not isinstance(t_str, str):
        return '00:00:00'

    t_str = t_str.strip().lower()

    # Case 1: HH:MM:SS
    match_hms = re.match(r'(\d{1,2}):(\d{2}):(\d{2})', t_str)
    if match_hms:
        h = int(match_hms.group(1))
        m = int(match_hms.group(2))
        s = int(match_hms.group(3))
        return f"{h:02d}:{m:02d}:{s:02d}"

    # Case 2: HH:MM
    match_hm = re.match(r'(\d{1,2}):(\d{2})', t_str)
    if match_hm:
        h = int(match_hm.group(1))
        m = int(match_hm.group(2))
        return f"{h:02d}:{m:02d}:00"

    # Case 3: Duration (e.g., "1 hour 57 min", "51 min", "1 hour")
    total_seconds = 0
    
    # "X hour Y min"
    match_hour_min = re.search(r'(\d+)\s*hour(?:s)?\s*(\d+)\s*min(?:ute)?s?', t_str)
    if match_hour_min:
        hours = int(match_hour_min.group(1))
        minutes = int(match_hour_min.group(2))
        total_seconds = hours * 3600 + minutes * 60
    else:
        # "X min"
        match_min = re.search(r'(\d+)\s*min(?:ute)?s?', t_str)
        if match_min:
            minutes = int(match_min.group(1))
            total_seconds = minutes * 60
        # "X hour"
        match_hour = re.search(r'(\d+)\s*hour(?:s)?', t_str)
        if match_hour:
            hours = int(match_hour.group(1))
            total_seconds = hours * 3600

    # Convert total_seconds to HH:MM:SS string
    if total_seconds > 0:
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    # Default if format is completely off or not covered
    return '00:00:00'


def insert_data_from_df(df: pd.DataFrame, table_name: str, column_mapping: dict):
    """
    Generic function to insert data into a specified table using a column mapping.
    Performs aggressive normalization on CSV column names to handle subtle discrepancies.
    Handles NOT NULL constraints by filling NaN/None values appropriately.
    """
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        normalized_df_columns_map = {
            normalize_column_name(col): col
            for col in df.columns
        }

        df_transformed = pd.DataFrame() # Use a new DataFrame to build
        db_columns_ordered = list(column_mapping.values())

        # Map CSV columns to DB columns and handle missing columns with defaults
        for csv_col_key_in_mapping, db_col in column_mapping.items():
            normalized_expected_csv_col = normalize_column_name(csv_col_key_in_mapping)
            original_csv_col_name = normalized_df_columns_map.get(normalized_expected_csv_col)

            if original_csv_col_name is not None and original_csv_col_name in df.columns:
                df_transformed.loc[:, db_col] = df[original_csv_col_name].copy()
            else:
                # Provide default values for missing columns
                sql_type = get_sql_type(db_col)
                if db_col in ['receipt_number', 'menu_name']:
                    df_transformed.loc[:, db_col] = ''
                elif sql_type == "REAL":
                    df_transformed.loc[:, db_col] = 0.0
                elif sql_type == "INTEGER":
                    df_transformed.loc[:, db_col] = 0
                else:
                    df_transformed.loc[:, db_col] = np.nan # Use np.nan for other types, then fill later

        # Ensure all columns in db_columns_ordered are present in df_transformed before processing
        for col in db_columns_ordered:
            if col not in df_transformed.columns:
                sql_type = get_sql_type(col)
                if col in ['receipt_number', 'menu_name']:
                    df_transformed.loc[:, col] = ''
                elif sql_type == "REAL":
                    df_transformed.loc[:, col] = 0.0
                elif sql_type == "INTEGER":
                    df_transformed.loc[:, col] = 0
                else:
                    df_transformed.loc[:, col] = np.nan


        # Data Cleaning/Transformation and handling NOT NULL
        for col in db_columns_ordered: # Iterate through target DB columns
            if col not in df_transformed.columns:
                continue # Should not happen if previous block is correct

            # Use .loc to avoid SettingWithCopyWarning
            with pd.option_context('mode.chained_assignment', None): # Suppress SettingWithCopyWarning for this block
                if get_sql_type(col) == "REAL":
                    df_transformed.loc[:, col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0.0)
                elif get_sql_type(col) == "INTEGER":
                    df_transformed.loc[:, col] = pd.to_numeric(df_transformed[col], errors='coerce').fillna(0).astype(int)
                elif get_sql_type(col) == "TEXT":
                    df_transformed.loc[:, col] = df_transformed[col].astype(str).fillna('')
        
        # Specific date/time formatting for storage as TEXT
        if 'payment_date' in df_transformed.columns:
            def parse_date_robust_and_format(date_str):
                if pd.isna(date_str) or not isinstance(date_str, str):
                    return '' # Return empty string for invalid dates for SQLite TEXT storage
                
                dt_obj = pd.NaT
                # Try common formats first
                for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y']: # Added %m/%d/%Y just in case
                    try:
                        dt_obj = pd.to_datetime(date_str, format=fmt, errors='coerce')
                        if not pd.isna(dt_obj):
                            break
                    except ValueError:
                        pass # Continue to next format if this fails
                
                if pd.isna(dt_obj): # If still NaT, try general parsing
                    dt_obj = pd.to_datetime(date_str, errors='coerce')

                if pd.isna(dt_obj):
                    return '' # If all parsing fails, return empty string
                else:
                    return dt_obj.strftime('%Y-%m-%d') # Format to YYYY-MM-DD for consistent storage

            df_transformed.loc[:, 'payment_date'] = df_transformed['payment_date'].apply(parse_date_robust_and_format)

        if 'payment_time' in df_transformed.columns:
            # Use the new robust time formatting function
            df_transformed.loc[:, 'payment_time'] = df_transformed['payment_time'].apply(format_time_string_for_storage)
        else:
            df_transformed.loc[:, 'payment_time'] = '00:00:00'

        # Select only the columns that exist in the database schema and in the transformed DataFrame
        final_df_to_insert = df_transformed[[col for col in db_columns_ordered if col in df_transformed.columns]]

        # Ensure all columns expected by the DB are present, even if data is null/default
        # This step is crucial for `executemany` to match column count
        for col in db_columns_ordered:
            if col not in final_df_to_insert.columns:
                sql_type = get_sql_type(col)
                if sql_type == "TEXT":
                    final_df_to_insert.loc[:, col] = ''
                elif sql_type == "REAL":
                    final_df_to_insert.loc[:, col] = 0.0
                elif sql_type == "INTEGER":
                    final_df_to_insert.loc[:, col] = 0

        # Convert to list of lists for sqlite executemany
        data_to_insert = final_df_to_insert[db_columns_ordered].values.tolist() # Ensure order matches

        quoted_columns = [f'"{col}"' for col in db_columns_ordered]
        placeholders = ', '.join(['?' for _ in db_columns_ordered])
        sql_insert_query = f"INSERT INTO \"{table_name}\" ({', '.join(quoted_columns)}) VALUES ({placeholders})"

        cursor.executemany(sql_insert_query, data_to_insert)
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error importing data into '{table_name}' (SQLite Error): {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during import into '{table_name}' (General Error): {e}")
        # Print full traceback for debugging
        import traceback
        traceback.print_exc()
        return False
    finally:
        if conn:
            conn.close()

# Wrapper functions for clarity in app.py
def insert_bills_data(df: pd.DataFrame):
    return insert_data_from_df(df, 'bills', BILLS_COLUMN_MAPPING)

def insert_bill_details_data(df: pd.DataFrame):
    return insert_data_from_df(df, 'bill_details', DETAIL_BILLS_COLUMN_MAPPING)

def get_all_bills():
    """
    Retrieves all bill data from the 'bills' table.
    Ensures data types are clean for Streamlit display.
    """
    conn = None
    df = pd.DataFrame()
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql_query("SELECT * FROM bills", conn)

        # Convert to datetime only for display/internal processing where needed
        # Stored as text in DB for robustness, convert back for analysis/display
        if 'payment_date' in df.columns:
            # Note: We are reading from DB where it's already YYYY-MM-DD string
            df.loc[:, 'payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
        if 'created_at' in df.columns:
            df.loc[:, 'created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        if 'payment_time' in df.columns:
            # Payment time is stored as HH:MM:SS string, keep as string or convert if needed
            df.loc[:, 'payment_time'] = df['payment_time'].astype(str).replace('nan', '')


        # Ensure numeric columns are actually numeric
        for col in df.columns:
            if col in [v for k,v in BILLS_COLUMN_MAPPING.items() if get_sql_type(v) == "REAL"]:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            elif col in [v for k,v in BILLS_COLUMN_MAPPING.items() if get_sql_type(v) == "INTEGER"]:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            elif col in [v for k,v in BILLS_COLUMN_MAPPING.items() if get_sql_type(v) == "TEXT"]:
                df.loc[:, col] = df[col].fillna('').astype(str)

        return df
    except pd.errors.DatabaseError as e:
        print(f"Pandas Database Error retrieving data from 'bills' table: {e}")
        return pd.DataFrame()
    except sqlite3.Error as e:
        print(f"SQLite Error retrieving data from 'bills' table: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred while retrieving all bills: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_all_bill_details():
    """
    Retrieves all bill detail data from the 'bill_details' table.
    Ensures data types are clean for Streamlit display.
    """
    conn = None
    df = pd.DataFrame()
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql_query("SELECT * FROM bill_details", conn)

        for col in df.columns:
            if col in [v for k,v in DETAIL_BILLS_COLUMN_MAPPING.items() if get_sql_type(v) == "REAL"]:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            elif col in [v for k,v in DETAIL_BILLS_COLUMN_MAPPING.items() if get_sql_type(v) == "INTEGER"]:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            elif col in [v for k,v in DETAIL_BILLS_COLUMN_MAPPING.items() if get_sql_type(v) == "TEXT"]:
                df.loc[:, col] = df[col].fillna('').astype(str)

        return df
    except pd.errors.DatabaseError as e:
        print(f"Pandas Database Error retrieving data from 'bill_details' table: {e}")
        return pd.DataFrame()
    except sqlite3.Error as e:
        print(f"SQLite Error retrieving data from 'bill_details' table: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred while retrieving all bill details: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# --- Functions for data analysis ---
def get_bills_for_analysis():
    """
    Retrieves necessary bill data for revenue and customer trend analysis.
    Ensures payment_date and payment_time are correctly parsed and cleaned for analysis.
    """
    conn = None
    df = pd.DataFrame()
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql_query("SELECT payment_date, payment_time, receipt_number, total_final_bill, seat_amount, payment_type, branch FROM bills", conn)

        if df.empty:
            print("No data fetched from bills table for analysis.")
            return pd.DataFrame()

        # 1. Convert payment_date (which is TEXT from DB, YYYY-MM-DD format) to datetime
        if 'payment_date' in df.columns:
            df.loc[:, 'payment_date_dt'] = pd.to_datetime(df['payment_date'], errors='coerce', format='%Y-%m-%d')
        else:
            print("Warning: 'payment_date' column not found for bills analysis. Returning empty DataFrame.")
            return pd.DataFrame()

        # 2. Convert payment_time (which is TEXT from DB, HH:MM:SS format) to Timedelta
        if 'payment_time' in df.columns:
            # We assume payment_time is already cleaned to HH:MM:SS format by insert_data_from_df
            df.loc[:, 'payment_time_td'] = pd.to_timedelta(df['payment_time'].fillna('00:00:00'), errors='coerce')
        else:
            print("Warning: 'payment_time' column not found for bills analysis. Defaulting to 0 duration.")
            df.loc[:, 'payment_time_td'] = pd.Timedelta(seconds=0)

        # 3. Drop rows where parsing failed for date or time
        df_cleaned = df.dropna(subset=['payment_date_dt', 'payment_time_td']).copy()

        if df_cleaned.empty:
            print("Warning: No valid payment_date or payment_time data remaining after cleaning for full_datetime creation.")
            return pd.DataFrame() # Return empty if no valid rows

        # 4. Combine payment_date_dt and payment_time_td into a single datetime column
        # Ensure 'payment_date_dt' is tz-naive before adding Timedelta
        df_cleaned.loc[:, 'full_datetime'] = df_cleaned['payment_date_dt'].dt.tz_localize(None) + df_cleaned['payment_time_td']
        
        # Final check: ensure 'full_datetime' is definitely datetime type and drop any remaining NaT
        df_cleaned.loc[:, 'full_datetime'] = pd.to_datetime(df_cleaned['full_datetime'], errors='coerce')
        df_cleaned.dropna(subset=['full_datetime'], inplace=True)


        # 5. Drop the temporary time delta and date columns
        df_cleaned = df_cleaned.drop(columns=['payment_time_td', 'payment_date_dt'])


        # 6. Convert total_final_bill to numeric, fill NaN with 0
        if 'total_final_bill' in df_cleaned.columns:
            df_cleaned.loc[:, 'total_final_bill'] = pd.to_numeric(df_cleaned['total_final_bill'], errors='coerce').fillna(0)
        else:
            df_cleaned.loc[:, 'total_final_bill'] = 0.0

        # 7. Ensure receipt_number is string and not null
        if 'receipt_number' in df_cleaned.columns:
            df_cleaned.loc[:, 'receipt_number'] = df_cleaned['receipt_number'].fillna('').astype(str)
        else:
            df_cleaned.loc[:, 'receipt_number'] = ''

        # 8. Ensure seat_amount is numeric and fill NaN
        if 'seat_amount' in df_cleaned.columns:
            df_cleaned.loc[:, 'seat_amount'] = pd.to_numeric(df_cleaned['seat_amount'], errors='coerce').fillna(0).astype(int)
        else:
            df_cleaned.loc[:, 'seat_amount'] = 0

        # 9. Ensure payment_type and branch are strings
        if 'payment_type' in df_cleaned.columns:
            df_cleaned.loc[:, 'payment_type'] = df_cleaned['payment_type'].fillna('').astype(str)
        else:
            df_cleaned.loc[:, 'payment_type'] = ''

        if 'branch' in df_cleaned.columns:
            df_cleaned.loc[:, 'branch'] = df_cleaned['branch'].fillna('').astype(str)
        else:
            df_cleaned.loc[:, 'branch'] = ''

        return df_cleaned # Return the cleaned DataFrame
    
    except sqlite3.Error as e:
        print(f"Error retrieving bills for analysis: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred during bills retrieval for analysis: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for debugging
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_bill_details_for_analysis():
    """
    Retrieves necessary bill detail data for menu analysis and combo analysis.
    Ensures quantity is numeric and menu_name/receipt_number are clean strings.
    """
    conn = None
    df_details = pd.DataFrame()
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        # Fetch bill details with necessary columns for menu analysis and combo analysis
        df_details = pd.read_sql_query("SELECT receipt_number, menu_name, quantity, price_per_unit, discounted_price, summary_price, discount_by_item FROM bill_details", conn)
        
        if df_details.empty:
            return pd.DataFrame()

        # Convert quantity to numeric, fill NaN with 0
        if 'quantity' in df_details.columns:
            df_details.loc[:, 'quantity'] = pd.to_numeric(df_details['quantity'], errors='coerce').fillna(0)
        else:
            df_details.loc[:, 'quantity'] = 0.0

        # Convert price_per_unit, discounted_price, summary_price, discount_by_item to numeric, fill NaN with 0
        for col in ['price_per_unit', 'discounted_price', 'summary_price', 'discount_by_item']:
            if col in df_details.columns:
                df_details.loc[:, col] = pd.to_numeric(df_details[col], errors='coerce').fillna(0.0)
            else:
                df_details.loc[:, col] = 0.0 # Add column if missing with default 0.0

        # Ensure receipt_number and menu_name are strings and not null
        if 'receipt_number' in df_details.columns:
            df_details.loc[:, 'receipt_number'] = df_details['receipt_number'].fillna('').astype(str)
        else:
            df_details.loc[:, 'receipt_number'] = ''

        if 'menu_name' in df_details.columns:
            df_details.loc[:, 'menu_name'] = df_details['menu_name'].fillna('').astype(str)
        else:
            df_details.loc[:, 'menu_name'] = ''

        # Filter out invalid menu names (e.g., empty strings or 'nan') if any
        df_details_cleaned = df_details[df_details['menu_name'] != ''].copy()
        df_details_cleaned.dropna(subset=['receipt_number', 'menu_name'], inplace=True)


        return df_details_cleaned
    except sqlite3.Error as e:
        print(f"SQLite Error retrieving bill details for analysis: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred during bill details retrieval for analysis: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def get_basket_data(exclude_items: list = None): # Modified to accept exclude_items
    """
    Prepares data in a one-hot encoded format suitable for association rule mining.
    Each row represents a transaction (receipt_number), and columns are menu items.
    """
    df_details = get_bill_details_for_analysis()
    if df_details.empty:
        return pd.DataFrame()

    if exclude_items is None:
        exclude_items = [] # Default to empty list if not provided

    df_filtered = df_details[~df_details['menu_name'].isin(exclude_items)].copy()

    if df_filtered.empty:
        print("Warning: No relevant food items remaining after filtering for basket analysis.")
        return pd.DataFrame()

    # Ensure each item in a receipt is unique for basket analysis
    transactions = df_filtered.groupby('receipt_number')['menu_name'].apply(lambda x: list(set(x))).reset_index()

    # Use TransactionEncoder to convert to one-hot encoded DataFrame
    try:
        from mlxtend.preprocessing import TransactionEncoder
        te = TransactionEncoder()
        te_ary = te.fit(transactions['menu_name']).transform(transactions['menu_name'])
        df_onehot = pd.DataFrame(te_ary, columns=te.columns_)
        
        return df_onehot
    except ImportError:
        print("Error: mlxtend not installed. Please install with 'pip install mlxtend'")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error preparing basket data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


if __name__ == '__main__':
    # This block runs when db_manager.py is executed directly
    print("Creating tables...")
    create_bills_table()
    create_bill_details_table()
    print("Tables created/ensured.")
    print("Remember to delete 'foodstory_bills.db' if you change column mappings or table schemas significantly and want to re-import all data.")