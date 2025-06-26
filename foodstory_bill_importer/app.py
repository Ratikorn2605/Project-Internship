import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import locale # For Thai locale for day names

# For association rule mining
from mlxtend.frequent_patterns import apriori, association_rules

# Set Thai locale for day names
try:
    locale.setlocale(locale.LC_ALL, 'th_TH.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Thai_Thailand.65001') # For Windows
    except locale.Error:
        st.warning("Could not set Thai locale for day names. Day names might be in English.")


# Import functions from db_manager
from db_manager import (
    create_bills_table,
    create_bill_details_table,
    insert_bills_data,
    insert_bill_details_data,
    get_all_bills,
    get_all_bill_details,
    get_bills_for_analysis,
    get_bill_details_for_analysis,
    get_basket_data # New import for combo analysis
)

# Initialize database tables
create_bills_table()
create_bill_details_table()

st.set_page_config(layout="wide")

# --- Page Title ---
st.title("ระบบนำเข้าและวิเคราะห์ข้อมูลบิล Foodstory")

# --- Data Import Section ---
st.header("นำเข้าข้อมูล")

col1, col2 = st.columns(2)

with col1:
    st.subheader("นำเข้าข้อมูลบิลหลัก (Bills)")
    uploaded_bills_file = st.file_uploader("ลากและวางไฟล์ CSV 'Bills' ที่นี่", type=["csv"], key="bills_uploader")

    if uploaded_bills_file is not None:
        try:
            df_bills_raw = pd.read_csv(uploaded_bills_file)
            st.write("ตัวอย่างข้อมูลบิลหลัก:")
            st.dataframe(df_bills_raw.head())

            if st.button("นำเข้าข้อมูลบิลหลักไปยัง SQLite", key="import_bills_btn"):
                if insert_bills_data(df_bills_raw):
                    st.success("นำเข้าข้อมูลบิลหลักสำเร็จแล้ว!")
                else:
                    st.error("นำเข้าข้อมูลบิลหลักไม่สำเร็จ กรุณาตรวจสอบข้อผิดพลาดในคอนโซลสำหรับรายละเอียดและตรวจสอบให้แน่ใจว่าชื่อคอลัมน์ตรงกับการแมปใน `db_manager.py`")
        except Exception as e:
            st.error(f"เกิดข้อผิดพลาดในการอ่านไฟล์ Bills: {e}")

with col2:
    st.subheader("นำเข้าข้อมูลรายละเอียดบิล (Bill Details)")
    uploaded_details_file = st.file_uploader("ลากและวางไฟล์ CSV 'Bill Details' ที่นี่", type=["csv"], key="details_uploader")

    if uploaded_details_file is not None:
        try:
            df_details_raw = pd.read_csv(uploaded_details_file)
            st.write("ตัวอย่างข้อมูลรายละเอียดบิล:")
            st.dataframe(df_details_raw.head())

            if st.button("นำเข้าข้อมูลรายละเอียดบิลไปยัง SQLite", key="import_details_btn"):
                if insert_bill_details_data(df_details_raw):
                    st.success("นำเข้าข้อมูลรายละเอียดบิลสำเร็จแล้ว!")
                else:
                    st.error("นำเข้าข้อมูลรายละเอียดบิลไม่สำเร็จ กรุณาตรวจสอบข้อผิดพลาดในคอนโซลสำหรับรายละเอียดและตรวจสอบให้แน่ใจว่าชื่อคอลัมน์ตรงกับการแมปใน `db_manager.py`")
        except Exception as e:
            st.error(f"เกิดข้อผิดพลาดในการอ่านไฟล์ Bill Details: {e}")

st.markdown("---")

# --- Data Overview Section ---
st.header("ภาพรวมข้อมูล")

if st.button("ดึงข้อมูลล่าสุดจากฐานข้อมูล"):
    st.session_state['bills_data'] = get_all_bills()
    st.session_state['bill_details_data'] = get_all_bill_details()
    st.success("ดึงข้อมูลสำเร็จแล้ว!")

if 'bills_data' not in st.session_state:
    st.session_state['bills_data'] = get_all_bills()
if 'bill_details_data' not in st.session_state:
    st.session_state['bill_details_data'] = get_all_bill_details()

bills_df = st.session_state['bills_data']
bill_details_df = st.session_state['bill_details_data']

st.subheader("ตารางบิลหลัก")
if not bills_df.empty:
    st.dataframe(bills_df)
    st.write(f"จำนวนรายการทั้งหมด: {len(bills_df):,} รายการ")
else:
    st.info("ยังไม่มีข้อมูลบิลหลักในฐานข้อมูล กรุณานำเข้าข้อมูลก่อน")

st.subheader("ตารางรายละเอียดบิล")
if not bill_details_df.empty:
    st.dataframe(bill_details_df)
    st.write(f"จำนวนรายการทั้งหมด: {len(bill_details_df):,} รายการ")
else:
    st.info("ยังไม่มีข้อมูลรายละเอียดบิลในฐานข้อมูล กรุณานำเข้าข้อมูลก่อน")

st.markdown("---")

# --- Basic Analysis Section ---
st.header("การวิเคราะห์ข้อมูลพื้นฐาน")

df_bills_analysis = get_bills_for_analysis()

if not df_bills_analysis.empty:
    st.subheader("รายได้รวม (Revenue Analysis)")
    total_revenue = df_bills_analysis['total_final_bill'].sum()
    st.metric(label="รายได้รวมทั้งหมด", value=f"{total_revenue:,.2f} บาท")

    # Group by payment date for daily revenue
    daily_revenue = df_bills_analysis.groupby('payment_date')['total_final_bill'].sum().reset_index()
    daily_revenue.columns = ['Payment Date', 'Total Revenue']
    daily_revenue['Payment Date'] = pd.to_datetime(daily_revenue['Payment Date'])
    daily_revenue = daily_revenue.sort_values('Payment Date')

    if not daily_revenue.empty:
        st.write("### แนวโน้มรายได้รายวัน (Daily Revenue Trend)")
        fig_daily_revenue = px.line(daily_revenue, x='Payment Date', y='Total Revenue', 
                                    title='รายได้รวมรายวัน',
                                    labels={'Payment Date': 'วันที่', 'Total Revenue': 'รายได้รวม (บาท)'})
        fig_daily_revenue.update_traces(mode='lines+markers')
        fig_daily_revenue.update_xaxes(dtick="M1", tickformat="%d %b\n%Y") # Show month and year
        st.plotly_chart(fig_daily_revenue, use_container_width=True)
    else:
        st.info("ไม่พบข้อมูลรายได้รายวันสำหรับการวิเคราะห์.")


    # Daily Customer Trend (Bills)
    st.subheader("แนวโน้มลูกค้า (จำนวนบิล) รายวัน (Daily Customer Trend)")
    daily_bills_count = df_bills_analysis.groupby('payment_date').size().reset_index(name='Number of Bills')
    daily_bills_count.columns = ['Payment Date', 'Number of Bills']
    daily_bills_count['Payment Date'] = pd.to_datetime(daily_bills_count['Payment Date'])
    
    # Fill missing dates with 0 bills
    if not daily_bills_count.empty:
        min_date = daily_bills_count['Payment Date'].min()
        max_date = daily_bills_count['Payment Date'].max()
        all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
        full_daily_bills = pd.DataFrame(all_dates, columns=['Payment Date'])
        full_daily_bills = pd.merge(full_daily_bills, daily_bills_count, on='Payment Date', how='left').fillna(0)
        full_daily_bills = full_daily_bills.sort_values('Payment Date')

        fig_daily_customers = px.line(full_daily_bills, x='Payment Date', y='Number of Bills',
                                      title='จำนวนบิล/ลูกค้า รายวัน',
                                      labels={'Payment Date': 'วันที่', 'Number of Bills': 'จำนวนบิล/ลูกค้า'})
        fig_daily_customers.update_traces(mode='lines+markers')
        fig_daily_customers.update_xaxes(dtick="M1", tickformat="%d %b\n%Y")
        st.plotly_chart(fig_daily_customers, use_container_width=True)
        st.info("หมายเหตุ: ค่าศูนย์ในกราฟหมายถึงวันนั้นไม่มีการทำรายการบิลเกิดขึ้น (ยอดขายเป็นศูนย์) ไม่ได้หมายความว่าร้านปิดทำการ")
    else:
        st.info("ไม่พบข้อมูลจำนวนบิลรายวันสำหรับการวิเคราะห์.")


    # Average Number of Bills/Customers by Day of Week
    st.subheader("จำนวนบิล/ลูกค้า เฉลี่ยตามวันในสัปดาห์ (Average Number of Bills/Customers by Day of Week)")
    # Ensure full_datetime is indeed datetime, then extract day_name
    df_bills_analysis['day_of_week'] = df_bills_analysis['full_datetime'].dt.day_name()

    # Define order of days in Thai
    thai_day_order = ["จันทร์", "อังคาร", "พุธ", "พฤหัสบดี", "ศุกร์", "เสาร์", "อาทิตย์"]

    # Map English day names to Thai, handling potential locale issues
    # Ensure this mapping is robust regardless of locale.setlocale success
    english_to_thai_days = {
        "Monday": "จันทร์", "Tuesday": "อังคาร", "Wednesday": "พุธ",
        "Thursday": "พฤหัสบดี", "Friday": "ศุกร์", "Saturday": "เสาร์", "Sunday": "อาทิตย์"
    }
    # Apply mapping, defaulting to English if no match (though locale should handle it)
    df_bills_analysis['day_of_week_thai'] = df_bills_analysis['day_of_week'].map(english_to_thai_days).fillna(df_bills_analysis['day_of_week'])

    # Aggregate by day of week
    bills_by_day_of_week = df_bills_analysis.groupby('day_of_week_thai')['receipt_number'].count().reset_index(name='Total Bills')

    # Get the number of unique dates for each day of the week to calculate average
    date_counts_by_day = df_bills_analysis.groupby('day_of_week_thai')['payment_date'].nunique().reset_index(name='Unique Dates')
    
    # Merge to calculate average
    avg_bills_by_day = pd.merge(bills_by_day_of_week, date_counts_by_day, on='day_of_week_thai', how='left')
    avg_bills_by_day['Average Bills'] = avg_bills_by_day['Total Bills'] / avg_bills_by_day['Unique Dates']
    avg_bills_by_day.fillna(0, inplace=True) # Fill NaN if no bills for a day

    # Ensure correct order for the plot
    avg_bills_by_day['day_of_week_thai'] = pd.Categorical(avg_bills_by_day['day_of_week_thai'], categories=thai_day_order, ordered=True)
    avg_bills_by_day = avg_bills_by_day.sort_values('day_of_week_thai')


    if not avg_bills_by_day.empty:
        fig_avg_day = px.bar(avg_bills_by_day, x='day_of_week_thai', y='Average Bills',
                             title='จำนวนบิล/ลูกค้าเฉลี่ย ตามวันในสัปดาห์',
                             labels={'day_of_week_thai': 'วันในสัปดาห์', 'Average Bills': 'จำนวนบิล/ลูกค้าเฉลี่ย'},
                             text_auto=True) # Show values on bars
        st.plotly_chart(fig_avg_day, use_container_width=True)
    else:
        st.info("ไม่พบข้อมูลจำนวนบิล/ลูกค้าเฉลี่ยตามวันในสัปดาห์สำหรับการวิเคราะห์.")


    # Hourly Customer Trend
    st.subheader("แนวโน้มลูกค้า (จำนวนบิล) รายชั่วโมง (Hourly Customer Trend)")
    if not df_bills_analysis.empty and 'full_datetime' in df_bills_analysis.columns and pd.api.types.is_datetime64_any_dtype(df_bills_analysis['full_datetime']):
        df_bills_analysis['hour'] = df_bills_analysis['full_datetime'].dt.hour
        hourly_bills_count = df_bills_analysis.groupby('hour').size().reset_index(name='Number of Bills')
        
        # Fill in missing hours with 0
        all_hours = pd.DataFrame({'hour': range(24)})
        hourly_bills_count = pd.merge(all_hours, hourly_bills_count, on='hour', how='left').fillna(0)
        hourly_bills_count = hourly_bills_count.sort_values('hour')

        fig_hourly_customers = px.line(hourly_bills_count, x='hour', y='Number of Bills',
                                      title='จำนวนบิล/ลูกค้า ตามช่วงเวลา (ชั่วโมง)',
                                      labels={'hour': 'ชั่วโมง (0-23)', 'Number of Bills': 'จำนวนบิล/ลูกค้า'},
                                      markers=True)
        fig_hourly_customers.update_layout(xaxis = dict(tickmode = 'linear', dtick = 1)) # Show all hours
        st.plotly_chart(fig_hourly_customers, use_container_width=True)
    else:
        st.info("ข้อมูล 'full_datetime' ไม่เพียงพอหรือไม่ถูกต้องสำหรับการวิเคราะห์แนวโน้มลูกค้าตามช่วงเวลา กรุณาตรวจสอบคอลัมน์ 'payment_date' และ 'payment_time' ว่ามีข้อมูลที่ถูกต้อง.")


    st.subheader("ยอดขายตามประเภทการชำระเงิน (Revenue by Payment Type)")
    revenue_by_pay_type = df_bills_analysis.groupby('payment_type')['total_final_bill'].sum().reset_index()
    if not revenue_by_pay_type.empty:
        fig_pay_type = px.bar(revenue_by_pay_type, x='payment_type', y='total_final_bill',
                              title='ยอดขายตามประเภทการชำระเงิน',
                              labels={'payment_type': 'ประเภทการชำระเงิน', 'total_final_bill': 'ยอดขายรวม (บาท)'},
                              text_auto=True)
        st.plotly_chart(fig_pay_type, use_container_width=True)
    else:
        st.info("ไม่พบข้อมูลยอดขายตามประเภทการชำระเงินสำหรับการวิเคราะห์.")

    st.subheader("ยอดขายตามสาขา (Revenue by Branch)")
    if 'branch' in df_bills_analysis.columns and not df_bills_analysis['branch'].replace('', np.nan).dropna().empty: # Check if branch column exists and has non-empty values
        revenue_by_branch = df_bills_analysis.groupby('branch')['total_final_bill'].sum().reset_index()
        if not revenue_by_branch.empty:
            fig_branch = px.bar(revenue_by_branch, x='branch', y='total_final_bill',
                                  title='ยอดขายตามสาขา',
                                  labels={'branch': 'สาขา', 'total_final_bill': 'ยอดขายรวม (บาท)'},
                                  text_auto=True)
            st.plotly_chart(fig_branch, use_container_width=True)
        else:
            st.info("ไม่พบข้อมูลยอดขายตามสาขาสำหรับการวิเคราะห์.")
    else:
        st.info("ไม่พบข้อมูลสาขาในข้อมูลบิลหลักสำหรับการวิเคราะห์ยอดขายตามสาขา หรือคอลัมน์ 'branch' ไม่มีข้อมูล.")

else:
    st.info("ไม่พบข้อมูลบิลสำหรับการวิเคราะห์พื้นฐาน กรุณานำเข้าข้อมูลก่อน")

st.markdown("---")

# --- Menu Analysis Section ---
st.header("การวิเคราะห์เมนู (Menu Analysis)")

df_bill_details_analysis = get_bill_details_for_analysis()

if not df_bill_details_analysis.empty:
    # --- Menu Exclusion for Analysis ---
    st.subheader("กำหนดเมนูที่จะยกเว้นในการวิเคราะห์เมนู")
    # Get all unique menu names for selection
    all_menu_names = sorted(df_bill_details_analysis['menu_name'].unique().tolist())
    
    # Pre-populate some common items to exclude
    default_exclude_items = ['น้ำเปล่า', 'น้ำแข็ง', 'ข้าวเปล่า', 'Soda', 'Coca Cola', 'Diet Coke', 'Sprite']
    
    selected_exclude_items = st.multiselect(
        "เลือกเมนูที่ต้องการยกเว้นจากการวิเคราะห์ (เช่น เครื่องดื่ม, น้ำเปล่า, ของแถม)",
        options=all_menu_names,
        default=[item for item in default_exclude_items if item in all_menu_names]
    )

    df_filtered_menu_analysis = df_bill_details_analysis[~df_bill_details_analysis['menu_name'].isin(selected_exclude_items)].copy()

    if df_filtered_menu_analysis.empty:
        st.warning("หลังจากกรองเมนูแล้ว ไม่มีข้อมูลเมนูเหลือสำหรับการวิเคราะห์เมนูขายดี")
    else:
        # Top N Selling Menu Items by Quantity
        st.subheader(f"เมนูขายดีที่สุด (ตามจำนวน) - ไม่รวม: {', '.join(selected_exclude_items) if selected_exclude_items else 'ไม่มี'}")
        top_n_quantity = st.slider("เลือกจำนวนเมนูขายดีที่ต้องการแสดง", 5, 20, 10, key="top_n_quantity_slider")
        
        menu_quantity = df_filtered_menu_analysis.groupby('menu_name')['quantity'].sum().nlargest(top_n_quantity).reset_index()
        if not menu_quantity.empty:
            fig_top_quantity = px.bar(menu_quantity, x='menu_name', y='quantity',
                                      title=f'{top_n_quantity} เมนูขายดีที่สุดตามจำนวน',
                                      labels={'menu_name': 'ชื่อเมนู', 'quantity': 'จำนวนที่ขายได้'},
                                      text_auto=True)
            st.plotly_chart(fig_top_quantity, use_container_width=True)
        else:
            st.info("ไม่พบข้อมูลเมนูสำหรับการวิเคราะห์เมนูขายดีที่สุด.")

        # Top N Selling Menu Items by Revenue
        st.subheader(f"เมนูที่สร้างรายได้สูงสุด (ตามยอดขาย) - ไม่รวม: {', '.join(selected_exclude_items) if selected_exclude_items else 'ไม่มี'}")
        top_n_revenue = st.slider("เลือกจำนวนเมนูที่สร้างรายได้สูงสุดที่ต้องการแสดง", 5, 20, 10, key="top_n_revenue_slider")

        menu_revenue = df_filtered_menu_analysis.groupby('menu_name')['summary_price'].sum().nlargest(top_n_revenue).reset_index()
        if not menu_revenue.empty:
            fig_top_revenue = px.bar(menu_revenue, x='menu_name', y='summary_price',
                                     title=f'{top_n_revenue} เมนูที่สร้างรายได้สูงสุด',
                                     labels={'menu_name': 'ชื่อเมนู', 'summary_price': 'รายได้รวม (บาท)'},
                                     text_auto=True)
            st.plotly_chart(fig_top_revenue, use_container_width=True)
        else:
            st.info("ไม่พบข้อมูลเมนูสำหรับการวิเคราะห์เมนูที่สร้างรายได้สูงสุด.")
else:
    st.info("ไม่พบข้อมูลรายละเอียดบิลสำหรับการวิเคราะห์เมนู กรุณานำเข้าข้อมูลก่อน")


st.markdown("---")

# --- Combo Analysis Section (Association Rule Mining) ---
st.header("การวิเคราะห์ชุดเมนู (Combo Analysis)")
st.info("การวิเคราะห์นี้จะช่วยให้คุณเข้าใจว่าเมนูใดมักถูกซื้อคู่กัน (Basket Analysis)")

# --- Combo Analysis Exclusion ---
st.subheader("กำหนดเมนูที่จะยกเว้นในการวิเคราะห์ชุดเมนู")
# Use the same list of all_menu_names from Menu Analysis section
# If that section is not run yet, ensure all_menu_names is populated
if 'df_bill_details_analysis' in locals() and not df_bill_details_analysis.empty:
    all_menu_names_for_combo = sorted(df_bill_details_analysis['menu_name'].unique().tolist())
else:
    all_menu_names_for_combo = [] # Or fetch it again if df_bill_details_analysis is not available

default_exclude_items_combo = ['น้ำเปล่า', 'น้ำแข็ง', 'ข้าวเปล่า', 'Soda', 'Coca Cola', 'Diet Coke', 'Sprite'] # Default suggestions

selected_exclude_items_combo = st.multiselect(
    "เลือกเมนูที่ต้องการยกเว้นจากการวิเคราะห์ชุดเมนู (เช่น เครื่องดื่ม, น้ำเปล่า)",
    options=all_menu_names_for_combo,
    default=[item for item in default_exclude_items_combo if item in all_menu_names_for_combo],
    key="combo_exclude_items"
)

# Pass the selected excluded items to get_basket_data
basket_sets = get_basket_data(exclude_items=selected_exclude_items_combo)

if not basket_sets.empty:
    st.write("### ตารางข้อมูลสำหรับ Basket Analysis (ตัวอย่าง)")
    st.dataframe(basket_sets.head())
    st.write(f"จำนวนรายการ (บิล) ทั้งหมดที่ใช้ในการวิเคราะห์: {len(basket_sets):,} รายการ")
    st.write(f"จำนวนเมนูทั้งหมดที่ใช้ในการวิเคราะห์: {len(basket_sets.columns):,} เมนู")


    min_support = st.slider("เลือก Minimum Support (%) สำหรับ Apriori", 0.1, 10.0, 0.5, 0.1) / 100
    st.write(f"Selected Minimum Support: {min_support:.2%}")

    try:
        frequent_itemsets = apriori(basket_sets, min_support=min_support, use_colnames=True)
        if not frequent_itemsets.empty:
            frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
            st.write("### ชุดเมนูที่พบบ่อย (Frequent Itemsets)")
            st.dataframe(frequent_itemsets.sort_values(by='support', ascending=False).head(10))

            min_confidence = st.slider("เลือก Minimum Confidence (%) สำหรับ Association Rules", 10.0, 100.0, 70.0, 5.0) / 100
            st.write(f"Selected Minimum Confidence: {min_confidence:.2%}")

            rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
            
            # Sort by lift for better insights
            rules = rules.sort_values(by=['lift', 'confidence'], ascending=[False, False]).reset_index(drop=True)

            if not rules.empty:
                st.write("### กฎความสัมพันธ์ของเมนู (Association Rules)")
                # Format itemsets for better display
                rules["antecedents"] = rules["antecedents"].apply(lambda x: ', '.join(list(x)))
                rules["consequents"] = rules["consequents"].apply(lambda x: ', '.join(list(x)))
                
                # Display relevant columns
                st.dataframe(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head(10))
                st.info("คำอธิบาย: \n"
                        "- **Support:** ความบ่อยครั้งที่ชุดเมนูนี้ปรากฏในบิล (ยิ่งสูงยิ่งบ่อย)\n"
                        "- **Confidence:** ความน่าจะเป็นที่เมนู Consequents จะถูกซื้อ ถ้าเมนู Antecedents ถูกซื้อ (ยิ่งสูงยิ่งมีความสัมพันธ์แข็งแกร่ง)\n"
                        "- **Lift:** ตัวบ่งชี้ความสัมพันธ์ที่แท้จริง (ค่า > 1 แสดงว่ามีการซื้อร่วมกันมากกว่าความบังเอิญ)")
            else:
                st.info("ไม่พบกฎความสัมพันธ์ของเมนูภายใต้เงื่อนไข Support และ Confidence ที่เลือก ลองปรับค่าลดลง.")
        else:
            st.info("ไม่พบชุดเมนูที่พบบ่อย (Frequent Itemsets) ภายใต้เงื่อนไข Support ที่เลือก ลองปรับค่า Support ลดลง.")
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดในการวิเคราะห์ชุดเมนู (Basket Analysis): {e}")
        st.info("ตรวจสอบให้แน่ใจว่าได้ติดตั้ง `mlxtend` แล้ว (pip install mlxtend) และข้อมูลที่นำเข้ามีเมนูที่แตกต่างกันเพียงพอ.")
else:
    st.info("ไม่พบข้อมูลรายละเอียดบิลสำหรับ Basket Analysis กรุณานำเข้าข้อมูลรายละเอียดบิลก่อน.")