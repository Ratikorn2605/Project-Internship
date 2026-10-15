import sqlite3
import pandas as pd
import os
import re
import unicodedata
import numpy as np


DATABASE_NAME = 'foodstory_bills.db'


def normalize_column_name(col_name: str) -> str:
    """
    Aggressively normalizes column names for robust matching:
    1. Converts to NFKC Unicode form.
    2. Removes all whitespace, tabs, and newlines.
    3. Removes non-alphanumeric characters.
    """
    if not isinstance(col_name, str):
        return ""
    normalized = unicodedata.normalize('NFKC', col_name)
    # Remove all whitespace/tabs/newlines
    normalized = re.sub(r'\s+', '', normalized)

    if "inv.no" in normalized.lower() or "custompaymentref." in normalized.lower():
        normalized = re.sub(r'[^\w.]', '', normalized)
    else:
        normalized = re.sub(r'[^\w]', '', normalized)

    return normalized.lower()


BILLS_COLUMN_MAPPING = {
    'Payment Date': 'payment_date',
    'Payment Time': 'payment_time',
    'Time': 'time_col',
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
    'Voucher Amount': 'v_amt_desc',
    'Voucher Discount': 'voucher_discount',
    'Rounding amount': 'rounding_amt',
    'Delivery Fee': 'delivery_fee',
    'Total': 'total_final_bill',
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
    'Branch': 'branch'
}


def get_sql_type(column_name):
    numeric_cols = [
        'summary_price', 'discount_by_item', 'subtotal_bill_discount',
        'subtotal_summary_price_minus_discount_by_item', 'service_charge',
        'non_vat', 'ex_vat', 'before_vat_subtotal_plus_service_charge',
        'vat', 'v_amt_desc', 'voucher_discount', 'rounding_amt',
        'delivery_fee', 'total_final_bill', 'tips', 'quantity', 'price_per_unit',
        'discount_by_item_percent', 'discounted_price'
    ]
    integer_cols = ['seat_amount']

    if column_name in numeric_cols:
        return "REAL"
    elif column_name in integer_cols:
        return "INTEGER"
    else:
        return "TEXT"


def create_table_from_mapping(table_name: str, column_mapping: dict):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        columns_sql_parts = []
        for db_col in column_mapping.values():
            col_type = get_sql_type(db_col)
            if db_col == 'receipt_number':
                columns_sql_parts.append(f'"{db_col}" {col_type} UNIQUE')
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
        print(f"Error ensuring table '{table_name}' exists: {e}")
    finally:
        if conn:
            conn.close()


def create_bills_table():
    create_table_from_mapping('bills', BILLS_COLUMN_MAPPING)


def create_bill_details_table():
    create_table_from_mapping('bill_details', DETAIL_BILLS_COLUMN_MAPPING)


def find_matching_column(csv_columns, key_hint, db_col_name):
    """
    ค้นหาคอลัมน์จาก CSV โดยเปรียบเทียบชื่อที่ Normalize แล้ว
    """
    normalized_csv_map = {normalize_column_name(col): col for col in csv_columns}

    # 1. ลอง Match ตรงๆ กับ key_hint
    norm_key = normalize_column_name(key_hint)
    if norm_key in normalized_csv_map:
        return normalized_csv_map[norm_key]

    # 2. กรณีพิเศษสำหรับ total_final_bill
    if db_col_name == 'total_final_bill':
        for norm_csv_col, original_col in normalized_csv_map.items():
            if 'total' in norm_csv_col and ('nonvat' in norm_csv_col or 'vat' in norm_csv_col or 'rounding' in norm_csv_col):
                return original_col

    # 3. ลองเทียบ Partial Match
    for norm_csv_col, original_col in normalized_csv_map.items():
        if norm_key and norm_key in norm_csv_col:
            return original_col

    return None


def insert_data_from_df(df: pd.DataFrame, table_name: str, column_mapping: dict):
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        df_renamed = pd.DataFrame()

        for csv_key, db_col in column_mapping.items():
            matched_col = find_matching_column(df.columns, csv_key, db_col)
            if matched_col is not None:
                df_renamed[db_col] = df[matched_col]
            else:
                df_renamed[db_col] = None

        # --- กรองแถวที่เป็นค่าว่าง / NaN ออกจาก menu_name สำหรับตาราง bill_details ---
        if table_name == 'bill_details' and 'menu_name' in df_renamed.columns:
            # แปลงเป็นสตริงเพื่อเช็คค่าว่าง สเปซ หรือข้อความ 'nan'/'none'/'null'
            menu_str = df_renamed['menu_name'].astype(str).str.strip().str.lower()
            valid_mask = (
                df_renamed['menu_name'].notnull() & 
                (menu_str != '') & 
                (~menu_str.isin(['nan', 'none', 'null']))
            )
            df_renamed = df_renamed[valid_mask]

        # Clean & Convert Data Types
        for db_col in df_renamed.columns:
            sql_type = get_sql_type(db_col)

            if sql_type == "REAL":
                cleaned = df_renamed[db_col].astype(str).str.replace(',', '', regex=False).str.strip()
                df_renamed[db_col] = pd.to_numeric(cleaned, errors='coerce').fillna(0.0)
            elif sql_type == "INTEGER":
                cleaned = df_renamed[db_col].astype(str).str.replace(',', '', regex=False).str.strip()
                df_renamed[db_col] = pd.to_numeric(cleaned, errors='coerce').fillna(0).astype(int)
            else:
                df_renamed[db_col] = df_renamed[db_col].fillna('').astype(str)

        db_columns_ordered = list(column_mapping.values())
        df_to_insert = df_renamed[db_columns_ordered]

        data_to_insert = df_to_insert.values.tolist()

        quoted_columns = [f'"{col}"' for col in db_columns_ordered]
        placeholders = ', '.join(['?'] * len(db_columns_ordered))
        sql_insert_query = f'INSERT OR REPLACE INTO "{table_name}" ({", ".join(quoted_columns)}) VALUES ({placeholders})'

        cursor.executemany(sql_insert_query, data_to_insert)
        conn.commit()
        return True

    except Exception as e:
        print(f"Error importing data into '{table_name}': {e}")
        return False
    finally:
        if conn:
            conn.close()


def insert_bills_data(df: pd.DataFrame):
    return insert_data_from_df(df, 'bills', BILLS_COLUMN_MAPPING)


def insert_bill_details_data(df: pd.DataFrame):
    return insert_data_from_df(df, 'bill_details', DETAIL_BILLS_COLUMN_MAPPING)


def get_all_bills():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql_query("SELECT * FROM bills", conn)
        return df
    except sqlite3.Error as e:
        print(f"Error retrieving data from 'bills' table: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def get_all_bill_details():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql_query("SELECT * FROM bill_details", conn)
        return df
    except sqlite3.Error as e:
        print(f"Error retrieving data from 'bill_details' table: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


create_bills_table()
create_bill_details_table()