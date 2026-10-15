import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import datetime
from db_manager import (
    create_bills_table, create_bill_details_table,
    insert_bills_data, insert_bill_details_data,
    get_all_bills, get_all_bill_details
)

# Initialize database tables
create_bills_table()
create_bill_details_table()

st.set_page_config(
    page_title="Foodstory Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# MODERN & PROFESSIONAL CUSTOM CSS (UX/UI)
# ==========================================
st.markdown("""
    <style>
    /* Main Background */
    .stApp {
        background-color: #F8FAFC;
    }
    
    /* Global Typography */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }

    /* Metric Cards Styling */
    .kpi-card {
        background-color: #FFFFFF;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #E2E8F0;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .kpi-title {
        font-size: 13px;
        font-weight: 600;
        color: #64748B;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 8px;
    }
    .kpi-value {
        font-size: 26px;
        font-weight: 700;
        color: #0F172A;
    }
    
    /* Clean Tab Headers */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #F1F5F9;
        padding: 6px;
        border-radius: 10px;
        width: fit-content;
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        border-radius: 8px;
        padding: 0 20px;
        font-weight: 600;
        font-size: 14px;
        color: #475569;
        border: none !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FFFFFF !important;
        color: #2563EB !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    /* Section Containers */
    .section-container {
        background-color: #FFFFFF;
        border-radius: 12px;
        padding: 24px;
        border: 1px solid #E2E8F0;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);
        margin-bottom: 20px;
    }
    .section-title {
        font-size: 18px;
        font-weight: 700;
        color: #0F172A;
        margin-bottom: 16px;
    }
    
    /* Custom Buttons */
    .stButton>button {
        border-radius: 8px;
        height: 2.8em;
        font-weight: 600;
        background-color: #2563EB;
        color: white;
        border: none;
    }
    .stButton>button:hover {
        background-color: #1D4ED8;
        color: white;
    }
    </style>
""", unsafe_allow_html=True)

# Application Header
st.title("📊 Foodstory Sales Dashboard")

tab_dash, tab_import, tab_raw = st.tabs(["📊 Overview Dashboard", "📥 Import Data", "📋 Raw Data Explorer"])

# ==========================================
# TAB 1: DASHBOARD
# ==========================================
with tab_dash:
    df_bills = get_all_bills()
    df_details = get_all_bill_details()

    if not df_bills.empty:
        # Date Processing
        df_bills['payment_date_dt'] = pd.to_datetime(df_bills['payment_date'], dayfirst=True, errors='coerce')
        valid_dates = df_bills['payment_date_dt'].dropna()
        
        min_date = valid_dates.min().date() if not valid_dates.empty else datetime.today().date()
        max_date = valid_dates.max().date() if not valid_dates.empty else datetime.today().date()

        # Modern Filter Section Above Dashboard
        st.markdown("<div class='section-container'>", unsafe_allow_html=True)
        f_col1, f_col2 = st.columns([2, 1])
        with f_col1:
            st.markdown("##### 🗓️ ตัวกรองช่วงเวลา (Date Filter)")
            date_range = st.date_input(
                "เลือกช่วงวันที่ต้องการวิเคราะห์",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key="dashboard_date_filter_top"
            )
        with f_col2:
            st.write("")
            st.write("")
            if st.button("🔄 รีเซ็ตช่วงเวลาทั้งหมด", key="reset_date"):
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

        # Apply Date Filter
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
            df_bills_filtered = df_bills[
                (df_bills['payment_date_dt'].dt.date >= start_date) & 
                (df_bills['payment_date_dt'].dt.date <= end_date)
            ]
        else:
            start_date, end_date = min_date, max_date
            df_bills_filtered = df_bills.copy()

        # Details Filter
        if not df_details.empty:
            valid_receipts = df_bills_filtered['receipt_number'].astype(str).str.strip().unique()
            df_details['receipt_clean'] = df_details['receipt_number'].astype(str).str.strip()
            df_details_filtered = df_details[df_details['receipt_clean'].isin(valid_receipts)].copy()
            
            if df_details_filtered.empty:
                df_details_filtered = df_details.copy()
        else:
            df_details_filtered = pd.DataFrame()

        # Key Metrics KPI Section
        total_revenue = df_bills_filtered['total_final_bill'].sum()
        total_bills = len(df_bills_filtered)
        avg_bill = total_revenue / total_bills if total_bills > 0 else 0
        unique_days = df_bills_filtered['payment_date'].nunique()

        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        with m_col1:
            st.markdown(f"""
                <div class='kpi-card'>
                    <div class='kpi-title'>💰 ยอดขายรวม (Revenue)</div>
                    <div class='kpi-value'>฿{total_revenue:,.2f}</div>
                </div>
            """, unsafe_allow_html=True)
        with m_col2:
            st.markdown(f"""
                <div class='kpi-card'>
                    <div class='kpi-title'>🧾 จำนวนบิลทั้งหมด (Bills)</div>
                    <div class='kpi-value'>{total_bills:,} บิล</div>
                </div>
            """, unsafe_allow_html=True)
        with m_col3:
            st.markdown(f"""
                <div class='kpi-card'>
                    <div class='kpi-title'>📊 ยอดขายเฉลี่ย/บิล (Avg/Bill)</div>
                    <div class='kpi-value'>฿{avg_bill:,.2f}</div>
                </div>
            """, unsafe_allow_html=True)
        with m_col4:
            st.markdown(f"""
                <div class='kpi-card'>
                    <div class='kpi-title'>📅 จำนวนวันที่เปิดขาย (Days)</div>
                    <div class='kpi-value'>{unique_days} วัน</div>
                </div>
            """, unsafe_allow_html=True)

        st.write("")

        # Charts Section
        c1, c2 = st.columns(2)

        with c1:
            st.markdown("<div class='section-container'>", unsafe_allow_html=True)
            st.markdown("<div class='section-title'>📈 แนวโน้มยอดขายรายวัน (Daily Sales Trend)</div>", unsafe_allow_html=True)
            
            daily_sales = df_bills_filtered.groupby('payment_date_dt')['total_final_bill'].sum().reset_index()
            daily_sales = daily_sales.sort_values('payment_date_dt')
            daily_sales['pct_change'] = daily_sales['total_final_bill'].pct_change() * 100
            
            daily_sales['hover_text'] = daily_sales.apply(
                lambda row: (
                    f"<b>วันที่:</b> {row['payment_date_dt'].strftime('%d/%m/%Y')}<br>"
                    f"<b>ยอดขาย:</b> ฿{row['total_final_bill']:,.2f}<br>"
                    f"<b>เปลี่ยนแปลง:</b> {row['pct_change']:+.2f}%" 
                    if pd.notnull(row['pct_change']) else 
                    f"<b>วันที่:</b> {row['payment_date_dt'].strftime('%d/%m/%Y')}<br>"
                    f"<b>ยอดขาย:</b> ฿{row['total_final_bill']:,.2f}"
                ),
                axis=1
            )
            
            fig_sales = px.line(
                daily_sales, 
                x='payment_date_dt', 
                y='total_final_bill', 
                labels={'total_final_bill': 'ยอดขาย (บาท)', 'payment_date_dt': 'วันที่'},
                template="plotly_white",
                markers=True
            )
            fig_sales.update_traces(
                line_color='#2563EB',
                line_width=2.5,
                marker=dict(size=6, color='#1E40AF'),
                hovertemplate="%{customdata}<extra></extra>",
                customdata=daily_sales['hover_text']
            )
            fig_sales.update_layout(
                margin=dict(l=10, r=10, t=10, b=10),
                height=350,
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor='#F1F5F9')
            )
            st.plotly_chart(fig_sales, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with c2:
            st.markdown("<div class='section-container'>", unsafe_allow_html=True)
            st.markdown("<div class='section-title'>💳 สัดส่วนช่องทางชำระเงิน (Payment Methods)</div>", unsafe_allow_html=True)
            
            df_pay_filtered = df_bills_filtered.dropna(subset=['payment_type']).copy()
            pay_str = df_pay_filtered['payment_type'].astype(str).str.strip().str.lower()
            df_pay_filtered = df_pay_filtered[
                (pay_str != '') & 
                (~pay_str.isin(['nan', 'none', 'null']))
            ]
            
            pay_dist = df_pay_filtered['payment_type'].value_counts().reset_index()
            pay_dist.columns = ['Method', 'Count']
            
            fig_pay = px.pie(
                pay_dist, 
                values='Count', 
                names='Method', 
                hole=0.5,
                color_discrete_sequence=['#3B82F6', '#10B981', '#F59E0B', '#6366F1', '#EC4899']
            )
            fig_pay.update_traces(
                textposition='inside',
                textinfo='percent+label'
            )
            fig_pay.update_layout(
                margin=dict(l=10, r=10, t=10, b=10),
                height=350,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_pay, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        # Top 10 Best Selling Items Section
        st.markdown("<div class='section-container'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>🏆 10 อันดับ เมนูขายดีที่สุด (Top 10 Best Sellers)</div>", unsafe_allow_html=True)
        
        if not df_details_filtered.empty:
            df_det_clean = df_details_filtered.dropna(subset=['menu_name']).copy()
            menu_str = df_det_clean['menu_name'].astype(str).str.strip().str.lower()
            
            valid_mask = (
                (menu_str != '') & 
                (~menu_str.isin(['nan', 'none', 'null'])) &
                (df_det_clean['quantity'] > 0)
            )
            df_det_clean = df_det_clean[valid_mask]
            
            total_qty_all = df_det_clean['quantity'].sum()
            
            top_items = (
                df_det_clean.groupby('menu_name')['quantity']
                .sum()
                .sort_values(ascending=False)
                .head(10)
                .reset_index()
            )
            
            if not top_items.empty and total_qty_all > 0:
                top_items['pct'] = (top_items['quantity'] / total_qty_all) * 100
                top_items['rank'] = range(1, len(top_items) + 1)
                
                top_items['display_name'] = top_items.apply(
                    lambda row: f"#{row['rank']}  {row['menu_name']}", axis=1
                )
                top_items['label_text'] = top_items.apply(
                    lambda row: f" <b>{row['quantity']:,}</b> ชิ้น ({row['pct']:.1f}%)", axis=1
                )
                
                top_items_plot = top_items.iloc[::-1]

                fig_top = px.bar(
                    top_items_plot, 
                    x='quantity', 
                    y='display_name', 
                    orientation='h',
                    text='label_text',
                    labels={'quantity': 'จำนวนที่ขายได้', 'display_name': 'ชื่อเมนู'},
                    color='quantity', 
                    color_continuous_scale=['#93C5FD', '#2563EB', '#1E3A8A']
                )
                fig_top.update_traces(
                    textposition='outside',
                    cliponaxis=False
                )
                fig_top.update_layout(
                    margin=dict(l=10, r=80, t=10, b=10),
                    height=420,
                    yaxis={'categoryorder': 'array', 'categoryarray': top_items_plot['display_name'].tolist()},
                    xaxis=dict(showgrid=True, gridcolor='#F1F5F9'),
                    coloraxis_showscale=False
                )
                st.plotly_chart(fig_top, use_container_width=True)
            else:
                st.info("ไม่พบรายการเมนูขายดีในช่วงเวลาที่เลือก")
        else:
            st.warning("⚠️ ยังไม่มีข้อมูลรายละเอียดบิล (bill_details)")
        st.markdown("</div>", unsafe_allow_html=True)

        # Export Report Section
        st.markdown("<div class='section-container'>", unsafe_allow_html=True)
        st.markdown("<div class='section-title'>📥 ส่งออกรายงานสรุป (Export Reports)</div>", unsafe_allow_html=True)
        ex_col1, ex_col2 = st.columns(2)
        
        with ex_col1:
            csv_sales = daily_sales[['payment_date_dt', 'total_final_bill', 'pct_change']].to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📄 ดาวน์โหลดรายงานยอดขายรายวัน (CSV)",
                data=csv_sales,
                file_name=f"daily_sales_{start_date}_to_{end_date}.csv",
                mime="text/csv",
                use_container_width=True
            )
            
        with ex_col2:
            if not df_details_filtered.empty and 'top_items' in locals() and not top_items.empty:
                csv_top = top_items[['rank', 'menu_name', 'quantity', 'pct']].to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📄 ดาวน์โหลดรายงานเมนูขายดี Top 10 (CSV)",
                    data=csv_top,
                    file_name=f"top_selling_{start_date}_to_{end_date}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        st.markdown("</div>", unsafe_allow_html=True)

    else:
        st.info("👋 ยินดีต้อนรับ! กรุณาไปที่แท็บ 'Import Data' เพื่อนำเข้าไฟล์ CSV และเริ่มต้นดูข้อมูลวิเคราะห์")

# ==========================================
# TAB 2: IMPORT DATA
# ==========================================
with tab_import:
    st.markdown("<div class='section-container'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>📥 นำเข้าไฟล์ข้อมูล (Import CSV Data)</div>", unsafe_allow_html=True)
    
    with st.expander("⚠️ จัดการ & ลบข้อมูลทั้งหมดในระบบ"):
        st.warning("คำเตือน: การกดปุ่มนี้จะลบข้อมูลบิลและรายละเอียดทั้งหมดออกจากฐานข้อมูล เพื่อเริ่มใส่ข้อมูลใหม่!")
        confirm_delete = st.checkbox("ยืนยันว่าต้องการลบข้อมูลทั้งหมดในฐานข้อมูล")
        
        if st.button("🗑️ Clear All Database Data", key="clear_data_btn", disabled=not confirm_delete):
            try:
                conn = sqlite3.connect('foodstory_bills.db')
                cursor = conn.cursor()
                cursor.execute("DELETE FROM bill_details")
                cursor.execute("DELETE FROM bills")
                conn.commit()
                conn.close()
                
                st.success("✅ ลบข้อมูลทั้งหมดเรียบร้อยแล้ว! สามารถอัปโหลดไฟล์ใหม่ได้ทันที")
                st.rerun()
            except Exception as e:
                st.error(f"เกิดข้อผิดพลาดในการลบข้อมูล: {e}")

    st.markdown("---")
    
    col_up1, col_up2 = st.columns(2)
    with col_up1:
        st.markdown("##### ขั้นตอนที่ 1: อัปโหลดไฟล์บิลหลัก (`bills_*.csv`)")
        uploaded_bill_files = st.file_uploader("เลือกไฟล์ Bills CSV", type=["csv"], key="bill_uploader", accept_multiple_files=True)

        if uploaded_bill_files:
            st.success(f"เลือกไว้ {len(uploaded_bill_files)} ไฟล์")
            if st.button("🚀 นำเข้าข้อมูลบิลหลัก", key="import_bills_btn"):
                success_count = 0
                progress_bar = st.progress(0)
                for i, file in enumerate(uploaded_bill_files):
                    try:
                        df_upload = pd.read_csv(file, encoding='utf-8-sig')
                        if insert_bills_data(df_upload):
                            success_count += 1
                        progress_bar.progress((i + 1) / len(uploaded_bill_files))
                    except Exception as e:
                        st.error(f"เกิดข้อผิดพลาดกับไฟล์ {file.name}: {e}")
                
                if success_count > 0:
                    st.success(f"✅ นำเข้าสำเร็จ {success_count} ไฟล์!")
                    st.rerun()

    with col_up2:
        st.markdown("##### ขั้นตอนที่ 2: อัปโหลดรายละเอียดบิล (`detail_bill_*.csv`)")
        uploaded_detail_files = st.file_uploader("เลือกไฟล์ Bill Details CSV", type=["csv"], key="detail_uploader", accept_multiple_files=True)

        if uploaded_detail_files:
            st.success(f"เลือกไว้ {len(uploaded_detail_files)} ไฟล์")
            if st.button("🚀 นำเข้าข้อมูลรายละเอียดบิล", key="import_details_btn"):
                success_count = 0
                progress_bar = st.progress(0)
                for i, file in enumerate(uploaded_detail_files):
                    try:
                        df_upload = pd.read_csv(file, encoding='utf-8-sig')
                        if insert_bill_details_data(df_upload):
                            success_count += 1
                        progress_bar.progress((i + 1) / len(uploaded_detail_files))
                    except Exception as e:
                        st.error(f"เกิดข้อผิดพลาดกับไฟล์ {file.name}: {e}")
                
                if success_count > 0:
                    st.success(f"✅ นำเข้าสำเร็จ {success_count} ไฟล์!")
                    st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

# ==========================================
# TAB 3: RAW DATA
# ==========================================
with tab_raw:
    st.markdown("<div class='section-container'>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>📋 สำรวจตารางข้อมูลดิบ (Database Explorer)</div>", unsafe_allow_html=True)
    
    st.markdown("##### 1. ตารางบิลหลัก (Main Bills Table)")
    all_bills = get_all_bills()
    if not all_bills.empty:
        st.dataframe(all_bills, use_container_width=True, height=250)
    else:
        st.write("ไม่พบข้อมูล")

    st.markdown("---")
    
    st.markdown("##### 2. ตารางรายละเอียดบิล (Bill Details Table)")
    all_details = get_all_bill_details()
    if not all_details.empty:
        st.dataframe(all_details, use_container_width=True, height=250)
    else:
        st.write("ไม่พบข้อมูล")
    st.markdown("</div>", unsafe_allow_html=True)

# ==========================================
# SIDEBAR SYSTEM STATUS
# ==========================================
st.sidebar.header("⚙️ สถานะระบบ (System Status)")
if not get_all_bills().empty:
    st.sidebar.success("✅ มีข้อมูลในฐานข้อมูล")
else:
    st.sidebar.warning("⚠️ ยังไม่มีข้อมูลในฐานข้อมูล")

st.sidebar.markdown("---")
st.sidebar.caption("Foodstory Analytics Dashboard v2.0")