# main_fund_data.py
import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
from vnstock import Fund, Quote

# === Hàm cache để lấy và xử lý dữ liệu quỹ mở ===
@st.cache_data(show_spinner=True, ttl=60 * 30) # Giảm TTL xuống 30 phút vì dữ liệu NAV cập nhật hàng ngày
def get_fund_listing_cached(fund_type: str = ""):
    """Lấy danh sách quỹ và xử lý định dạng"""
    fund = Fund()
    try:
        df = fund.listing(fund_type=fund_type)
        if df.empty:
            return df
        # Danh sách cột phần trăm cần định dạng (ĐÃ BỔ SUNG nav_change_ytd)
        pct_cols = [
            'nav_change_previous', 'nav_change_last_year', 'nav_change_inception',
            'nav_change_1m', 'nav_change_3m', 'nav_change_6m',
            'nav_change_ytd', # <-- Bổ sung
            'nav_change_12m', 'nav_change_24m', 'nav_change_36m'
        ]
        # Định dạng các cột phần trăm
        for col in pct_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: f"{x:.2f}%" if not pd.isna(x) else "—")
        # Định dạng NAV
        if 'nav' in df.columns:
            df['nav'] = df['nav'].apply(lambda x: f"{x:,.0f}" if not pd.isna(x) else "—")
        # Định dạng phí quản lý
        if 'management_fee' in df.columns:
            df['management_fee'] = df['management_fee'].apply(lambda x: f"{x:.2f}%" if not pd.isna(x) else "—")
        return df
    except Exception as e:
        return pd.DataFrame()

# === Hàm cache để lấy dữ liệu chi tiết quỹ (NAV Report) ===
@st.cache_data(show_spinner=True, ttl=60 * 60 * 4) # Tăng TTL lên 4h vì NAV thường cập nhật 1 ngày/lần
def get_fund_nav_report_cached(symbol: str):
    """Lấy báo cáo NAV của quỹ"""
    fund = Fund()
    try:
        nav_df = fund.details.nav_report(symbol)
        if nav_df is not None and not nav_df.empty:
            nav_df['date'] = pd.to_datetime(nav_df['date'])
        return nav_df
    except Exception as e:
        return pd.DataFrame()

# === Hàm cache để lấy dữ liệu chi tiết quỹ (Top Holdings) ===
@st.cache_data(show_spinner=True, ttl=60 * 60 * 12) # Tăng TTL lên 12h vì thông tin này thay đổi không quá nhanh
def get_fund_top_holdings_cached(symbol: str):
    """Lấy danh mục đầu tư lớn nhất của quỹ"""
    fund = Fund()
    try:
        return fund.details.top_holding(symbol)
    except Exception as e:
        return pd.DataFrame()

# === Hàm cache để lấy dữ liệu chi tiết quỹ (Industry Holdings) ===
@st.cache_data(show_spinner=True, ttl=60 * 60 * 12) # Tăng TTL lên 12h
def get_fund_industry_holdings_cached(symbol: str):
    """Lấy phân bổ theo ngành của quỹ"""
    fund = Fund()
    try:
        return fund.details.industry_holding(symbol)
    except Exception as e:
        return pd.DataFrame()

# === Hàm cache để lấy dữ liệu chi tiết quỹ (Asset Holdings) ===
@st.cache_data(show_spinner=True, ttl=60 * 60 * 12) # Tăng TTL lên 12h
def get_fund_asset_holdings_cached(symbol: str):
    """Lấy phân bổ theo loại tài sản của quỹ"""
    fund = Fund()
    try:
        raw_df = fund.details.asset_holding(symbol)
        if raw_df is not None and not raw_df.empty:
            df = raw_df.copy() # Tạo bản sao để tránh cảnh báo SettingWithCopyWarning
            if 'short_name' not in df.columns:
                df['short_name'] = symbol
            return df
        return raw_df
    except Exception as e:
        return pd.DataFrame()

# === Hàm cache để lấy dữ liệu lịch sử của chỉ số thị trường ===
@st.cache_data(show_spinner=True, ttl=60 * 60 * 2) # TTL 2 giờ cho dữ liệu chỉ số
def get_market_index_history_cached(index_symbol: str, start_date: str, end_date: str):
    """Lấy dữ liệu lịch sử giá của chỉ số thị trường"""
    try:
        quote = Quote(symbol=index_symbol, source='VCI')
        df = quote.history(start=start_date, end=end_date, interval='1D', to_df=True)
        if df is not None and not df.empty:
            df['time'] = pd.to_datetime(df['time'])
            if 'close' in df.columns:
                df = df.rename(columns={'close': 'close_price'})
            df['symbol'] = index_symbol
            df = df[['time', 'close_price', 'symbol']].copy()
        return df
    except Exception as e:
        st.warning(f"Lỗi khi lấy dữ liệu lịch sử cho chỉ số {index_symbol}: {e}")
        return pd.DataFrame()

# === Hàm hỗ trợ định dạng ===
def fmt_pct(x):
    if pd.isna(x): return "—"
    return f"{x * 100:.1f}%"

def fmt_money_vnd(x):
    try:
        x = float(x)
    except Exception:
        return ""
    if pd.isna(x):
        return "—"
    if x >= 1e12:
        return f"{x / 1e12:.2f} nghìn tỷ"
    return f"{x / 1e9:.1f} tỷ"

# === Cấu hình trang ===
st.set_page_config(page_title="Dữ liệu Quỹ Mở (Streamlit)", layout="wide")
st.title("   📊    Hệ thống Phân tích Dữ liệu Quỹ Mở")
st.caption("Dữ liệu được cung cấp bởi Fmarket thông qua thư viện vnstock. Cập nhật theo ngày NAV công bố.")

# Bộ lọc
st.subheader("🎯 Bộ lọc")
fund_type_options = {
    "Tất cả quỹ": "",
    "Quỹ cổ phiếu": "STOCK",
    "Quỹ trái phiếu": "BOND",
    "Quỹ cân bằng": "BALANCED"
}
selected_type_label = st.selectbox(
    "Loại quỹ",
    options=list(fund_type_options.keys()),
    index=0
)
selected_type = fund_type_options[selected_type_label]

# Nút tải lại dữ liệu
refresh_fund_btn = st.button("🔄 Tải lại dữ liệu quỹ")

# --- Hiển thị dữ liệu danh sách quỹ ---
try:
    if refresh_fund_btn:
        get_fund_listing_cached.clear()
    fund_data = get_fund_listing_cached(fund_type=selected_type)
    if fund_data.empty:
        st.info("Không có dữ liệu quỹ mở để hiển thị với bộ lọc hiện tại.")
    else:
        # Chọn cột để hiển thị
        display_columns = [
            'short_name', 'name', 'fund_type', 'fund_owner_name',
            'management_fee', 'nav', 'nav_change_previous',
            'nav_change_1m', 'nav_change_3m',
            'nav_change_ytd',
            'nav_change_12m',
            'nav_update_at'
        ]
        column_names_vietnamese = {
            'short_name': 'Mã Quỹ',
            'name': 'Tên Quỹ',
            'fund_type': 'Loại Quỹ',
            'fund_owner_name': 'Công ty Quản lý',
            'management_fee': 'Phí quản lý',
            'nav': 'NAV/unit (VNĐ)',
            'nav_change_previous': 'Tăng trưởng 1N',
            'nav_change_1m': 'Tăng trưởng 1T',
            'nav_change_3m': 'Tăng trưởng 3T',
            'nav_change_ytd': 'Tăng trưởng YTD',
            'nav_change_12m': 'Tăng trưởng 1Năm',
            'nav_update_at': 'Ngày cập nhật NAV'
        }
        available_display_columns = [col for col in display_columns if col in fund_data.columns]
        display_df = fund_data[available_display_columns].rename(columns=column_names_vietnamese)

        # --- Hiển thị bảng danh sách quỹ ---
        st.subheader("📋 Danh sách Quỹ")
        display_df_reset = display_df.reset_index(drop=True)
        display_df_reset.index = display_df_reset.index + 1
        st.dataframe(display_df_reset, use_container_width=True, height=500)

        # --- Thêm phần chọn quỹ để xem chi tiết ---
        st.markdown("---")
        st.subheader("🔍 Chọn Quỹ để Xem Chi Tiết")
        if 'Mã Quỹ' in display_df.columns:
            fund_options = display_df.apply(lambda row: f"{row['Mã Quỹ']} - {row.get('Tên Quỹ', 'N/A')}", axis=1).tolist()
            selected_fund_option = st.selectbox(
                "Chọn một quỹ:",
                options=fund_options,
                index=0,
                key="fund_detail_selector"
            )
            if selected_fund_option:
                selected_fund_shortname = selected_fund_option.split(" - ")[0]
                st.markdown("---")
                st.subheader(f"📈 Chi tiết Quỹ: {selected_fund_option}")
                # 1. Báo cáo tăng trưởng NAV
                st.write("**1. Báo cáo tăng trưởng NAV (Giá Trị Tài Sản Ròng trên mỗi đơn vị quỹ)**")
                nav_report_df = get_fund_nav_report_cached(selected_fund_shortname)
                if nav_report_df is None or nav_report_df.empty:
                    st.info("Không có dữ liệu báo cáo NAV cho quỹ này.")
                else:
                    with st.expander("Xem dữ liệu NAV gần đây"):
                        nav_display = nav_report_df.tail(20).reset_index(drop=True)
                        nav_display.index = nav_display.index + 1
                        st.dataframe(nav_display[['date', 'nav_per_unit']], use_container_width=True)
                    nav_df_sorted = nav_report_df.sort_values('date').reset_index(drop=True)
                    if len(nav_df_sorted) < 2:
                        st.info("Không đủ dữ liệu để vẽ biểu đồ NAV (cần ít nhất 2 điểm dữ liệu).")
                    else:
                        latest_nav_all = nav_df_sorted['nav_per_unit'].iloc[-1]
                        first_nav_all = nav_df_sorted['nav_per_unit'].iloc[0]
                        total_growth_pct_all = ((latest_nav_all - first_nav_all) / first_nav_all) * 100 if first_nav_all != 0 else 0
                        time_periods = {
                            "3 tháng": 90,
                            "6 tháng": 180,
                            "12 tháng": 365,
                            "36 tháng": 3 * 365,
                            "Tất cả": "all"
                        }
                        period_selection = st.selectbox(
                            "Chọn khoảng thời gian biểu đồ NAV",
                            options=list(time_periods.keys()),
                            index=4,
                            key=f"nav_period_selector_{selected_fund_shortname}"
                        )
                        start_idx = 0
                        selected_data = nav_df_sorted
                        selected_growth_pct = total_growth_pct_all
                        if period_selection != "Tất cả":
                            days_needed = time_periods[period_selection]
                            if days_needed != "all":
                                latest_date = nav_df_sorted['date'].iloc[-1]
                                target_start_date = latest_date - pd.Timedelta(days=days_needed)
                                date_diffs = (nav_df_sorted['date'] - target_start_date).abs()
                                start_idx = date_diffs.idxmin()
                                selected_data = nav_df_sorted.iloc[start_idx:].copy()
                                if not selected_data.empty:
                                    selected_start_nav = selected_data['nav_per_unit'].iloc[0]
                                    selected_end_nav = selected_data['nav_per_unit'].iloc[-1]
                                    selected_growth_pct = ((selected_end_nav - selected_start_nav) / selected_start_nav) * 100 if selected_start_nav != 0 else 0
                                else:
                                    selected_growth_pct = 0
                                    st.warning(f"Không đủ dữ liệu để tính tăng trưởng cho khoảng thời gian {period_selection}.")
                        st.metric("Tăng trưởng NAV", f"{selected_growth_pct:+.2f}%", delta=None)
                        filtered_data = selected_data[['date', 'nav_per_unit']].copy()
                        fig_nav = px.line(
                            filtered_data,
                            x='date',
                            y='nav_per_unit',
                            title=f'Biến động NAV - {selected_fund_shortname} ({period_selection})',
                            labels={'date': 'Ngày', 'nav_per_unit': 'NAV trên mỗi đơn vị'}
                        )
                        fig_nav.update_xaxes(title_text='Ngày')
                        fig_nav.update_yaxes(title_text='NAV trên mỗi đơn vị')
                        min_val = float(filtered_data['nav_per_unit'].min())
                        max_val = float(filtered_data['nav_per_unit'].max())
                        step = 10000
                        ticks = np.arange(int(min_val // step) * step, int(max_val // step) * step + step, step)
                        tick_labels = [f"{int(tick/1000)}k" for tick in ticks]
                        fig_nav.update_yaxes(tickvals=ticks, ticktext=tick_labels)
                        fig_nav.update_layout(xaxis_rangeslider_visible=True)
                        st.plotly_chart(fig_nav, use_container_width=True)
                        latest_nav_row = nav_df_sorted.loc[nav_df_sorted['date'].idxmax()]
                        latest_nav = latest_nav_row['nav_per_unit']
                        latest_date = latest_nav_row['date']
                        st.metric("NAV gần nhất", f"{latest_nav:,.2f}", f"Ngày: {latest_date.strftime('%d/%m/%Y')}")

                # 2. Danh mục đầu tư lớn
                st.write("**2. Danh mục đầu tư lớn nhất**")
                top_holding_df = get_fund_top_holdings_cached(selected_fund_shortname)
                if top_holding_df is None or top_holding_df.empty:
                    st.info("Không có dữ liệu danh mục đầu tư lớn cho quỹ này.")
                else:
                    top_holding_vn_df = top_holding_df.rename(columns={
                        'stock_code': 'Mã cổ phiếu',
                        'industry': 'Ngành',
                        'net_asset_percent': 'Tỷ trọng tài sản ròng (%)'
                    })
                    top_holding_display = top_holding_vn_df[['Mã cổ phiếu', 'Ngành', 'Tỷ trọng tài sản ròng (%)']].reset_index(drop=True)
                    top_holding_display.index = top_holding_display.index + 1
                    st.dataframe(top_holding_display, use_container_width=True)
                    date_columns_to_check = ['update_at', 'report_date', 'updated_date', 'date']
                    update_date_str = "Không rõ"
                    for col in date_columns_to_check:
                        if col in top_holding_df.columns and not top_holding_df[col].isna().all():
                            try:
                                date_series = pd.to_datetime(top_holding_df[col], errors='coerce')
                                if not date_series.dropna().empty:
                                    latest_update_date = date_series.max()
                                    if pd.notna(latest_update_date):
                                        update_date_str = latest_update_date.strftime('%d/%m/%Y')
                                        break
                            except Exception:
                                continue
                    st.caption(f"Cập nhật đến ngày: {update_date_str}")
                    if 'net_asset_percent' in top_holding_df.columns and 'stock_code' in top_holding_df.columns:
                        top_holding_chart_df = top_holding_df[top_holding_df['net_asset_percent'] > 0].copy()
                        if not top_holding_chart_df.empty:
                            fig_top_stocks = px.pie(
                                top_holding_chart_df,
                                values='net_asset_percent',
                                names='stock_code',
                                title=f'Phân bổ tài sản theo mã cổ phiếu - {selected_fund_shortname}'
                            )
                            st.plotly_chart(fig_top_stocks, use_container_width=True)
                        else:
                            st.info("Không có dữ liệu hợp lệ để vẽ biểu đồ phân bổ theo mã cổ phiếu.")

                # 3. Phân bổ theo ngành
                st.write("**3. Phân bổ tài sản theo ngành**")
                industry_holding_df = get_fund_industry_holdings_cached(selected_fund_shortname)
                if industry_holding_df is None or industry_holding_df.empty:
                    st.info("Không có dữ liệu phân bổ theo ngành cho quỹ này.")
                else:
                    industry_holding_working_df = industry_holding_df.copy()
                    if 'short_name' not in industry_holding_working_df.columns:
                        industry_holding_working_df['short_name'] = selected_fund_shortname
                    industry_holding_vn_df = industry_holding_working_df.rename(columns={
                        'industry': 'Ngành',
                        'net_asset_percent': 'Tỷ trọng (%)'
                    })
                    columns_to_show = ['Ngành', 'Tỷ trọng (%)']
                    existing_columns_to_show = [col for col in columns_to_show if col in industry_holding_vn_df.columns]
                    industry_holding_display = industry_holding_vn_df[existing_columns_to_show].reset_index(drop=True)
                    industry_holding_display.index = industry_holding_display.index + 1
                    st.dataframe(industry_holding_display, use_container_width=True)
                    if 'net_asset_percent' in industry_holding_df.columns and 'industry' in industry_holding_df.columns:
                        industry_chart_df = industry_holding_df[industry_holding_df['net_asset_percent'] > 0].copy()
                        if not industry_chart_df.empty:
                            fig_industry = px.pie(
                                industry_chart_df,
                                values='net_asset_percent',
                                names='industry',
                                title=f'Phân bổ tài sản theo ngành - {selected_fund_shortname}'
                            )
                            st.plotly_chart(fig_industry, use_container_width=True)
                        else:
                            st.info("Không có dữ liệu hợp lệ để vẽ biểu đồ phân bổ theo ngành.")

                # 4. Phân bổ theo loại tài sản
                st.write("**4. Phân bổ tài sản**")
                asset_holding_df = get_fund_asset_holdings_cached(selected_fund_shortname)
                if asset_holding_df is None or asset_holding_df.empty:
                    st.info("Không có dữ liệu phân bổ theo loại tài sản cho quỹ này.")
                else:
                    asset_holding_working_df = asset_holding_df.copy()
                    if 'short_name' not in asset_holding_working_df.columns:
                        asset_holding_working_df['short_name'] = selected_fund_shortname
                    asset_holding_vn_df = asset_holding_working_df.rename(columns={
                        'asset_type': 'Loại tài sản',
                        'asset_percent': 'Tỷ trọng (%)'
                    })
                    columns_to_show_asset = ['Loại tài sản', 'Tỷ trọng (%)']
                    existing_columns_to_show_asset = [col for col in columns_to_show_asset if col in asset_holding_vn_df.columns]
                    asset_holding_display = asset_holding_vn_df[existing_columns_to_show_asset].reset_index(drop=True)
                    asset_holding_display.index = asset_holding_display.index + 1
                    st.dataframe(asset_holding_display, use_container_width=True)
                    if 'asset_percent' in asset_holding_df.columns and 'asset_type' in asset_holding_df.columns:
                        asset_chart_df = asset_holding_df[asset_holding_df['asset_percent'] > 0].copy()
                        if not asset_chart_df.empty:
                            fig_asset = px.pie(
                                asset_chart_df,
                                values='asset_percent',
                                names='asset_type',
                                title=f'Phân bổ tài sản theo loại - {selected_fund_shortname}'
                            )
                            st.plotly_chart(fig_asset, use_container_width=True)
                        else:
                            st.info("Không có dữ liệu hợp lệ để vẽ biểu đồ phân bổ theo loại tài sản.")

        # --- So sánh hiệu suất giữa các quỹ ---
        st.markdown("---")
        st.subheader("5. 📊 So sánh hiệu suất giữa các quỹ")
        if not fund_data.empty and 'short_name' in fund_data.columns:
            fund_code_to_name_map = fund_data.apply(lambda row: f"{row['short_name']} - {row.get('name', 'N/A')}", axis=1).to_dict()
            fund_codes_for_comparison = fund_data['short_name'].tolist()
            selected_fund_codes_for_comparison = st.multiselect(
                "Chọn tối đa 5 quỹ để so sánh (theo mã quỹ):",
                options=fund_codes_for_comparison,
                max_selections=5,
                format_func=lambda x: fund_code_to_name_map.get(x, x),
                key="fund_comparison_multiselect_tab_fund"
            )
            # --- Bổ sung lựa chọn khoảng thời gian ---
            col1, col2 = st.columns(2)
            with col1:
                start_date_funds = st.date_input(
                    "Ngày bắt đầu (Quỹ)",
                    value=datetime.today() - timedelta(days=365),
                    key="start_date_funds"
                )
            with col2:
                end_date_funds = st.date_input(
                    "Ngày kết thúc (Quỹ)",
                    value=datetime.today(),
                    key="end_date_funds"
                )
            if start_date_funds > end_date_funds:
                st.error('Ngày bắt đầu phải nhỏ hơn ngày kết thúc')
            elif selected_fund_codes_for_comparison:
                with st.spinner("Đang tải và xử lý dữ liệu NAV cho so sánh..."):
                    comparison_data_list = []
                    fund_with_insufficient_data = []
                    for fund_code in selected_fund_codes_for_comparison:
                        try:
                            nav_df = get_fund_nav_report_cached(fund_code)
                            if nav_df is not None and not nav_df.empty and 'date' in nav_df.columns and 'nav_per_unit' in nav_df.columns:
                                nav_df['date'] = pd.to_datetime(nav_df['date'])
                                nav_df = nav_df.sort_values('date').reset_index(drop=True)
                                # Lọc theo khoảng thời gian đã chọn
                                nav_df = nav_df[(nav_df['date'] >= pd.Timestamp(start_date_funds)) & (nav_df['date'] <= pd.Timestamp(end_date_funds))]
                                if len(nav_df) < 2:
                                    fund_with_insufficient_data.append(fund_code)
                                    continue
                                nav_df = nav_df.copy()
                                nav_df[f'cumulative_return_{fund_code}'] = (nav_df['nav_per_unit'] / nav_df['nav_per_unit'].iloc[0]) * 100
                                comparison_data_list.append(nav_df[['date', f'cumulative_return_{fund_code}']])
                            else:
                                fund_with_insufficient_data.append(fund_code)
                        except Exception as e:
                            st.warning(f"Lỗi khi xử lý dữ liệu NAV cho quỹ {fund_code}: {e}")
                            fund_with_insufficient_data.append(fund_code)
                    if fund_with_insufficient_data:
                        st.info(f"Các quỹ sau không có đủ dữ liệu trong khoảng thời gian đã chọn để so sánh: {', '.join(fund_with_insufficient_data)}")
                    if comparison_data_list:
                        try:
                            merged_df = comparison_data_list[0]
                            for df in comparison_data_list[1:]:
                                merged_df = pd.merge(merged_df, df, on='date', how='outer')
                            merged_df = merged_df.sort_values('date').reset_index(drop=True)
                            value_vars = [f'cumulative_return_{code}' for code in selected_fund_codes_for_comparison if f'cumulative_return_{code}' in merged_df.columns]
                            if value_vars:
                                plot_df = merged_df.melt(id_vars=['date'], value_vars=value_vars, var_name='Fund_Return_Series', value_name='Cumulative_Return')
                                plot_df['Fund_Code'] = plot_df['Fund_Return_Series'].str.replace('cumulative_return_', '', regex=False)
                                plot_df['Fund_Display_Name'] = plot_df['Fund_Code'].map(fund_code_to_name_map).fillna(plot_df['Fund_Code'])
                                fig_comparison = px.line(
                                    plot_df,
                                    x='date',
                                    y='Cumulative_Return',
                                    color='Fund_Display_Name',
                                    title='So sánh Hiệu suất Tích lũy của Các Quỹ (Base 100)',
                                    labels={'date': 'Ngày', 'Cumulative_Return': 'Giá trị Tích lũy (Base 100)', 'Fund_Display_Name': 'Quỹ'}
                                )
                                fig_comparison.update_layout(
                                    xaxis_title="Ngày",
                                    yaxis_title="Giá trị Tích lũy (Base 100)",
                                    legend_title="Quỹ"
                                )
                                st.plotly_chart(fig_comparison, use_container_width=True)
                                with st.expander("Xem dữ liệu chi tiết"):
                                    display_columns = ['date'] + value_vars
                                    display_df = merged_df[display_columns].copy()
                                    rename_dict = {f'cumulative_return_{code}': fund_code_to_name_map.get(code, code) for code in selected_fund_codes_for_comparison if f'cumulative_return_{code}' in merged_df.columns}
                                    display_df = display_df.rename(columns=rename_dict)
                                    display_df.index = display_df.index + 1
                                    st.dataframe(display_df, use_container_width=True)
                            else:
                                st.warning("Không có dữ liệu lợi suất tích lũy nào để vẽ biểu đồ.")
                        except Exception as e:
                            st.error(f"Lỗi khi kết hợp hoặc vẽ biểu đồ dữ liệu: {e}")
                    else:
                        st.info("Vui lòng chọn ít nhất một quỹ có dữ liệu hợp lệ trong khoảng thời gian đã chọn để so sánh.")
            else:
                st.info("Vui lòng chọn ít nhất một quỹ để bắt đầu so sánh.")
        else:
            st.warning("Không thể lấy danh sách quỹ để so sánh.")

        # --- So sánh hiệu suất quỹ với chỉ số thị trường ---
        st.markdown("---")
        st.subheader("6. 📈 So sánh hiệu suất các quỹ với chỉ số thị trường")
        market_indices = ['VNINDEX', 'VN30', 'HNXINDEX', 'UPCOMINDEX', 'HNX30']
        index_name_map = {
            'VNINDEX': 'Chỉ số VN-Index',
            'VN30': 'Chỉ số VN30',
            'HNXINDEX': 'Chỉ số HNX-Index',
            'UPCOMINDEX': 'Chỉ số UPCOM-Index',
            'HNX30': 'Chỉ số HNX30'
        }
        col1, col2 = st.columns(2)
        with col1:
            selected_fund_codes_for_index_comparison = st.multiselect(
                "Chọn quỹ để so sánh:",
                options=fund_codes_for_comparison,
                max_selections=5,
                format_func=lambda x: fund_code_to_name_map.get(x, x),
                key="fund_comparison_with_index_funds"
            )
        with col2:
            selected_indices_for_comparison = st.multiselect(
                "Chọn chỉ số thị trường để so sánh:",
                options=market_indices,
                max_selections=5,
                format_func=lambda x: index_name_map.get(x, x),
                key="fund_comparison_with_index_indices"
            )
            # --- Bổ sung lựa chọn khoảng thời gian ---
        col1_date, col2_date = st.columns(2)
        with col1_date:
            start_date_indices = st.date_input(
                "Ngày bắt đầu (Chỉ số)",
                value=datetime.today() - timedelta(days=365),
                key="start_date_indices"
            )
        with col2_date:
            end_date_indices = st.date_input(
                "Ngày kết thúc (Chỉ số)",
                value=datetime.today(),
                key="end_date_indices"
            )
        if start_date_indices > end_date_indices:
            st.error('Ngày bắt đầu phải nhỏ hơn ngày kết thúc')
        elif selected_fund_codes_for_index_comparison or selected_indices_for_comparison:
            all_symbols_to_compare = selected_fund_codes_for_index_comparison + selected_indices_for_comparison
            if len(all_symbols_to_compare) == 0:
                st.info("Vui lòng chọn ít nhất một quỹ hoặc một chỉ số.")
            else:
                with st.spinner("Đang tải dữ liệu lịch sử cho quỹ và chỉ số..."):
                    comparison_data_list_with_index = []
                    fund_with_insufficient_data_for_index = []
                    index_with_insufficient_data = []
                    # Lấy dữ liệu cho các quỹ được chọn
                    for fund_code in selected_fund_codes_for_index_comparison:
                        try:
                            nav_df = get_fund_nav_report_cached(fund_code)
                            if nav_df is not None and not nav_df.empty and 'date' in nav_df.columns and 'nav_per_unit' in nav_df.columns:
                                nav_df['date'] = pd.to_datetime(nav_df['date'])
                                nav_df = nav_df.sort_values('date').reset_index(drop=True)
                                # Lọc theo khoảng thời gian đã chọn
                                nav_df = nav_df[(nav_df['date'] >= pd.Timestamp(start_date_indices)) & (nav_df['date'] <= pd.Timestamp(end_date_indices))]
                                if len(nav_df) < 2:
                                    fund_with_insufficient_data_for_index.append(fund_code)
                                    continue
                                nav_df = nav_df.copy()
                                nav_df[f'cumulative_return_{fund_code}'] = (nav_df['nav_per_unit'] / nav_df['nav_per_unit'].iloc[0]) * 100
                                comparison_data_list_with_index.append(nav_df[['date', f'cumulative_return_{fund_code}']])
                            else:
                                fund_with_insufficient_data_for_index.append(fund_code)
                        except Exception as e:
                            st.warning(f"Lỗi khi xử lý dữ liệu NAV cho quỹ {fund_code}: {e}")
                            fund_with_insufficient_data_for_index.append(fund_code)
                    # Lấy dữ liệu cho các chỉ số được chọn
                    for index_code in selected_indices_for_comparison:
                        try:
                            index_df = get_market_index_history_cached(
                                index_symbol=index_code,
                                start_date=start_date_indices.strftime('%Y-%m-%d'),
                                end_date=end_date_indices.strftime('%Y-%m-%d')
                            )
                            if index_df is not None and not index_df.empty and 'time' in index_df.columns and 'close_price' in index_df.columns:
                                index_df['time'] = pd.to_datetime(index_df['time'])
                                index_df = index_df.sort_values('time').reset_index(drop=True)
                                # Lọc theo khoảng thời gian đã chọn (đã được lọc trong hàm get_market_index_history_cached, nhưng thêm lần nữa để chắc chắn)
                                index_df = index_df[(index_df['time'] >= pd.Timestamp(start_date_indices)) & (index_df['time'] <= pd.Timestamp(end_date_indices))]
                                if len(index_df) < 2:
                                    index_with_insufficient_data.append(index_code)
                                    continue
                                index_df = index_df.copy()
                                index_df[f'cumulative_return_{index_code}'] = (index_df['close_price'] / index_df['close_price'].iloc[0]) * 100
                                comparison_data_list_with_index.append(index_df[['time', f'cumulative_return_{index_code}']].rename(columns={'time': 'date'}))
                            else:
                                index_with_insufficient_data.append(index_code)
                        except Exception as e:
                            st.warning(f"Lỗi khi xử lý dữ liệu lịch sử cho chỉ số {index_code}: {e}")
                            index_with_insufficient_data.append(index_code)
                    if fund_with_insufficient_data_for_index:
                        st.info(f"Các quỹ sau không có đủ dữ liệu trong khoảng thời gian đã chọn để so sánh với chỉ số: {', '.join(fund_with_insufficient_data_for_index)}")
                    if index_with_insufficient_data:
                        st.info(f"Các chỉ số sau không có đủ dữ liệu trong khoảng thời gian đã chọn để so sánh: {', '.join(index_with_insufficient_data)}")
                    if comparison_data_list_with_index:
                        try:
                            merged_df_with_index = comparison_data_list_with_index[0]
                            for df in comparison_data_list_with_index[1:]:
                                merged_df_with_index = pd.merge(merged_df_with_index, df, on='date', how='outer')
                            merged_df_with_index = merged_df_with_index.sort_values('date').reset_index(drop=True)
                            date_cols = [col for col in merged_df_with_index.columns if col.startswith('cumulative_return_')]
                            if date_cols:
                                filtered_df = merged_df_with_index.dropna(subset=date_cols, how='all')
                                if not filtered_df.empty:
                                    merged_df_with_index = filtered_df.reset_index(drop=True)
                                else:
                                    st.warning("Không có dữ liệu hợp lệ nào sau khi lọc.")
                                    merged_df_with_index = pd.DataFrame()
                            else:
                                st.warning("Không tìm thấy cột dữ liệu lợi suất tích lũy nào.")
                                merged_df_with_index = pd.DataFrame()
                            if not merged_df_with_index.empty:
                                plot_df_with_index = merged_df_with_index.melt(id_vars=['date'], value_vars=date_cols, var_name='Series_Name', value_name='Cumulative_Return')
                                plot_df_with_index['Symbol'] = plot_df_with_index['Series_Name'].str.replace('cumulative_return_', '', regex=False)
                                def map_display_name(symbol):
                                    if symbol in fund_code_to_name_map:
                                        return fund_code_to_name_map[symbol]
                                    elif symbol in index_name_map:
                                        return index_name_map[symbol]
                                    else:
                                        return symbol
                                plot_df_with_index['Display_Name'] = plot_df_with_index['Symbol'].apply(map_display_name)
                                fig_comparison_with_index = px.line(
                                    plot_df_with_index,
                                    x='date',
                                    y='Cumulative_Return',
                                    color='Display_Name',
                                    title='So sánh Hiệu suất Tích lũy: Quỹ vs Chỉ số Thị trường (Base 100)',
                                    labels={'date': 'Ngày', 'Cumulative_Return': 'Giá trị Tích lũy (Base 100)', 'Display_Name': 'Tài sản'}
                                )
                                fig_comparison_with_index.update_layout(
                                    xaxis_title="Ngày",
                                    yaxis_title="Giá trị Tích lũy (Base 100)",
                                    legend_title="Tài sản"
                                )
                                st.plotly_chart(fig_comparison_with_index, use_container_width=True)
                                with st.expander("Xem dữ liệu chi tiết"):
                                    display_df_with_index = merged_df_with_index[date_cols + ['date']].copy()
                                    rename_dict_index = {col: map_display_name(col.replace('cumulative_return_', '')) for col in date_cols}
                                    display_df_with_index = display_df_with_index.rename(columns=rename_dict_index)
                                    display_df_with_index.index = display_df_with_index.index + 1
                                    st.dataframe(display_df_with_index, use_container_width=True)
                            else:
                                st.warning("Không có dữ liệu hợp lệ nào để vẽ biểu đồ sau khi xử lý.")
                        except Exception as e:
                            st.error(f"Lỗi khi kết hợp hoặc vẽ biểu đồ dữ liệu quỹ và chỉ số: {e}")
                    else:
                        st.info("Không có dữ liệu hợp lệ nào từ quỹ hoặc chỉ số trong khoảng thời gian đã chọn để so sánh.")
        else:
            st.info("Vui lòng chọn ít nhất một quỹ hoặc một chỉ số để bắt đầu so sánh.")
except Exception as e:
    st.error(f"Lỗi khi tải dữ liệu quỹ mở: {e}")

st.subheader("📝 Ghi chú")
st.markdown("""
- **NAV**: Giá trị tài sản ròng của quỹ trên mỗi đơn vị chứng chỉ quỹ (VNĐ).
- **Phí quản lý**: Phí được tính hàng năm trên giá trị tài sản của nhà đầu tư.
- **Tăng trưởng**: Tỷ suất sinh lời của quỹ trong các khoảng thời gian khác nhau, tính theo %.
- **1N**: 1 Ngày; **1T**: 1 Tháng; **3T**: 3 Tháng; **1Năm**: 12 Tháng.
- Dữ liệu chỉ mang tính chất tham khảo. Nhà đầu tư nên tìm hiểu kỹ trước khi quyết định.
""")
st.caption("(*) Dữ liệu quỹ mở được tổng hợp từ Fmarket thông qua thư viện vnstock. Có thể có độ trễ so với thời gian thực.")