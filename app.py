import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from copy import deepcopy
from plotly.subplots import make_subplots

# Set default Plotly theme to light white
import plotly.io as pio
pio.templates.default = "plotly_white"

# --- UNICODE ITALIC CONVERTER FOR TAXONOMIC NAMES ---
# For labels inside Plotly charts, dataframes etc. (st.markdown doesnt work in these cases)
def to_unicode_italic(text):
    """Converts standard A-Z, a-z letters to Unicode Mathematical Italic characters."""
    if not isinstance(text, str):
        return text
    result = []
    for char in text:
        code = ord(char)
        if 65 <= code <= 90:    # A-Z
            result.append(chr(0x1D608 + (code - 65)))
        elif 97 <= code <= 122:  # a-z
            result.append(chr(0x1D622 + (code - 97)))
        else:
            result.append(char)
    return "".join(result)

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Whale BioMonitor & Biodiversity Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CACHED DATA LOADING ---
SELECTED_COLUMNS = [
    'gbif_id',
    'species',
    'scientific_name',
    'family',
    'genus',
    'latitude',
    'longitude',
    'country_code',
    'occurrence_timestamp',
    'occurrence_year',
    'occurrence_month',
    'occurrence_season',
    'basis_of_record',
    'iucn_red_list_category',
    'conservation_severity'
]

@st.cache_data
def load_whale_data():
    base_dir = os.path.dirname(__file__)
    
    # Try finding parquet in data/ or root
    file_path = os.path.join(base_dir, "data", "fct_biodiversity_sightings.parquet")
    if not os.path.exists(file_path):
        file_path = os.path.join(base_dir, "fct_biodiversity_sightings.parquet")
    if not os.path.exists(file_path):
        file_path = "data/fct_biodiversity_sightings.parquet"

    df_raw = pd.read_parquet(file_path)
    
    existing_cols = [c for c in SELECTED_COLUMNS if c in df_raw.columns]
    df_clean = df_raw[existing_cols].copy()
    
    # Drop rows missing critical coordinates or species names
    req_cols = [c for c in ['latitude', 'longitude', 'species'] if c in df_clean.columns]
    if req_cols:
        df_clean = df_clean.dropna(subset=req_cols)
        
    # Merge common names mapping
    try:
        spp_path = os.path.join(base_dir, "data", "cetacea_species_mapping.csv")
        if not os.path.exists(spp_path):
            spp_path = "cetacea_species_mapping.csv"
        spp_mapping = pd.read_csv(spp_path)
        df_clean = df_clean.merge(spp_mapping[['species', 'common_name']], on='species', how='left')
        df_clean['common_name'] = df_clean['common_name'].fillna(df_clean['species'])
    except Exception:
        df_clean['common_name'] = df_clean['species']

    # Apply Unicode italics to species, genus, and family taxonomic names
    df_clean['scientific_italic'] = df_clean['species'].apply(to_unicode_italic)
    df_clean['family_italic'] = df_clean['family'].apply(to_unicode_italic)
    df_clean['genus_italic'] = df_clean['genus'].apply(to_unicode_italic)
    df_clean['species_display'] = df_clean['common_name'] + " (" + df_clean['scientific_italic'] + ")"

    # Merge country codes mapping
    try:
        country_path = os.path.join(base_dir, "data", "country_codes_mapping.csv")
        if not os.path.exists(country_path):
            country_path = "country_codes_mapping.csv"
        country_mapping = pd.read_csv(country_path)
        df_clean = df_clean.merge(country_mapping[['country_code', 'country_name']], on='country_code', how='left')
        df_clean['country_name'] = df_clean['country_name'].fillna(df_clean['country_code'])
    except Exception:
        df_clean['country_name'] = df_clean['country_code']

    df_clean['country_display'] = df_clean['country_name'] + " (" + df_clean['country_code'] + ")"

    return df_clean

# Load raw dataset once into cache
df_clean_raw = load_whale_data()
df_clean = deepcopy(df_clean_raw)

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("🐋 Whale BioMonitor")
st.sidebar.markdown("**Data Science & Bio-monitoring Dashboard**")

navigation = st.sidebar.radio(
    "Navigation / Pages:",
    [
        "Summary and dataset overview",
        "Interactive visualizations",
        "Data explorer"
    ]
)

# --- PAGE 1: EXECUTIVE SUMMARY & OVERVIEW WITH PLOTS & OPTIONAL TABLES ---
if navigation == "Summary and dataset overview":
    st.title("Summary and dataset overview")
    st.markdown("### Data Engineering Provenance & Key Metrics")
    
    # Overview Banner
    st.info("""
    **Data Pipeline Provenance:** This dataset proceedes from my Whale Biomonitor Data Engineering project. 
    Briefly, the project collects whale sightings data from the Global Biodiversity Information Facility (GBIF), 
    then orchestrates a data pipeline with **Apache Airflow** to automate extraction, loading into **Google Cloud Storage (GCS)**,
    and transformation using **dbt (data build tool)** in **Google BigQuery**.
    More details in the corresponding github repo: https://github.com/red-moonx/biomonitor-capstone
    """)

    # Lightweight Subtitle before KPI Cards
    st.markdown("##### *The dataset in a couple of numbers:*")

    # Top KPI Metrics (key performance indicators) wrapped in visual cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        with st.container(border=True):
            st.metric("Total Sightings", f"{len(df_clean):,}")
    with col2:
        with st.container(border=True):
            st.metric("Unique Species", f"{df_clean['species'].nunique()}")
    with col3:
        with st.container(border=True):
            st.metric("Unique Families", f"{df_clean['family'].nunique()}")
    with col4:
        with st.container(border=True):
            st.metric("Reporting Countries", f"{df_clean['country_code'].nunique()}")

    st.divider()

    # Section 1: Taxonomic breakdown (Plot + Optional Table)
    st.subheader("Taxonomic Breakdown")
    tax_col1, tax_col2 = st.columns(2)
    
    top_spp = df_clean.groupby(['common_name', 'scientific_italic']).size().reset_index(name='Sighting Count')
    top_spp = top_spp.sort_values(by='Sighting Count', ascending=False).head(10)
    top_spp.columns = ['Common Name', 'Scientific Name', 'Sighting Count']
    
    fam_df = df_clean['family_italic'].value_counts().reset_index()
    fam_df.columns = ['Taxonomic Family', 'Sighting Count']

    with tax_col1:
        st.markdown("**Top 10 most observed species**")
        fig_top_spp = px.bar(
            top_spp.sort_values(by='Sighting Count', ascending=True),
            x='Sighting Count',
            y='Common Name',
            orientation='h',
            color='Sighting Count',
            color_continuous_scale='Blues',
            text='Sighting Count'
        )
        fig_top_spp.update_layout(height=400, margin=dict(l=0, r=20, t=30, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig_top_spp, use_container_width=True)

    with tax_col2:
        st.markdown("**Sightings breakdown by family**")
        fig_fam = px.bar(
            fam_df.sort_values(by='Sighting Count', ascending=True),
            x='Sighting Count',
            y='Taxonomic Family',
            orientation='h',
            color='Sighting Count',
            color_continuous_scale='Teal',
            text='Sighting Count'
        )
        fig_fam.update_layout(height=400, margin=dict(l=0, r=20, t=30, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig_fam, use_container_width=True)

    # Checkbox for optional table display
    if st.checkbox("Show Taxonomic Data Tables"):
        tbl_c1, tbl_c2 = st.columns(2)
        with tbl_c1:
            st.dataframe(top_spp, use_container_width=True)
        with tbl_c2:
            st.dataframe(fam_df, use_container_width=True)

    st.divider()

    # Section 2: Geographic & Temporal Distribution (Plots + Optional Tables)
    st.subheader("Geographic and Temporal Distribution")
    geo_col1, geo_col2, geo_col3 = st.columns([1.1, 1, 1])

    # Country Sighting Calculations (Top 10)
    total_sightings_len = len(df_clean)
    country_counts = df_clean.groupby(['country_name', 'country_code']).size().reset_index(name='Sightings')
    top10_df = country_counts.sort_values(by='Sightings', ascending=False).head(10).copy()

    top10_df['Percentage'] = (top10_df['Sightings'] / total_sightings_len) * 100
    top10_df['Percentage_Str'] = top10_df['Percentage'].apply(lambda p: f"{p:.1f}%")

    year_df = df_clean['occurrence_year'].value_counts().sort_index().reset_index() if 'occurrence_year' in df_clean else pd.DataFrame()
    if not year_df.empty:
        year_df.columns = ['Year', 'Sightings']

    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month_df = df_clean['occurrence_month'].value_counts().sort_index().reset_index() if 'occurrence_month' in df_clean else pd.DataFrame()
    if not month_df.empty:
        month_df.columns = ['Month_Num', 'Sightings']
        month_df['Month'] = month_df['Month_Num'].apply(lambda m: month_names[int(m)-1] if 1 <= int(m) <= 12 else str(m))

    with geo_col1:
        st.markdown("**Top 10 Reporting Countries**")
        st.caption("&nbsp;", unsafe_allow_html=True)

        fig_geo = px.treemap(
            top10_df,
            path=['country_name'],
            values='Sightings',
            color='Sightings',
            color_continuous_scale='Blues',
            custom_data=['country_code', 'Percentage_Str']
        )
        fig_geo.update_traces(
            texttemplate="<b>%{label}</b><br>%{customdata[1]}",
            textfont=dict(size=13, family="sans-serif"),
            hovertemplate="<b>%{label}</b> (%{customdata[0]})<br>Sightings: %{value:,}<br>Share of Total: %{customdata[1]}<extra></extra>",
            marker=dict(pad=dict(t=4, l=4, r=4, b=4))
        )
        fig_geo.update_layout(
            height=350,
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_geo, use_container_width=True)

    with geo_col2:
        st.markdown("**Sightings by Year**")
        st.caption("*Data for 2026 is partial (up to March 2026)*")
        if not year_df.empty:
            fig_year = px.area(
                year_df,
                x='Year',
                y='Sightings',
                markers=True,
                color_discrete_sequence=['#0284c7']
            )
            fig_year.update_traces(fillcolor='rgba(2, 132, 199, 0.20)', line=dict(width=3, shape='spline'))
            fig_year.update_layout(height=350, margin=dict(l=0, r=20, t=30, b=0))
            st.plotly_chart(fig_year, use_container_width=True)

    with geo_col3:
        st.markdown("**Sightings by Month**")
        st.caption("&nbsp;", unsafe_allow_html=True)
        if not month_df.empty:
            fig_month = px.pie(
                month_df,
                names='Month',
                values='Sightings',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_month.update_traces(
                textinfo='label+percent',
                textposition='inside',
                textfont=dict(size=10, family="sans-serif"),
                hovertemplate="<b>%{label}</b><br>Sightings: %{value:,}<br>Percentage: %{percent}<extra></extra>"
            )
            fig_month.update_layout(
                height=350,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False
            )
            st.plotly_chart(fig_month, use_container_width=True)

    # Checkbox for optional table display
    if st.checkbox("Show Geographic & Temporal Data Tables"):
        gt_c1, gt_c2, gt_c3 = st.columns([1.1, 1, 1])
        with gt_c1:
            table_country_df = top10_df[['country_name', 'country_code', 'Sightings']].rename(
                columns={'country_name': 'Country Name', 'country_code': 'Country Code'}
            )
            st.dataframe(table_country_df, use_container_width=True)
        with gt_c2:
            st.dataframe(year_df, use_container_width=True)
        with gt_c3:
            st.dataframe(month_df[['Month', 'Sightings']], use_container_width=True)

    st.divider()

    # Section 3: Observation Methodology & IUCN Status (Plots + Optional Tables)
    st.subheader("3. Observation Methodology & IUCN Status")
    sec3_col1, sec3_col2, sec3_col3 = st.columns([1, 2, 1])

    # Observation Methodology Data Processing
    rec_raw = df_clean['basis_of_record'].value_counts().reset_index() if 'basis_of_record' in df_clean else pd.DataFrame()
    if not rec_raw.empty:
        rec_raw.columns = ['Methodology_Raw', 'Count']
        
        # Group into Human, Machine, Others
        def group_method(m):
            if m in ['HUMAN_OBSERVATION', 'MACHINE_OBSERVATION']:
                return m.replace('_', ' ').title()
            return 'Others'
        
        rec_raw['Group'] = rec_raw['Methodology_Raw'].apply(group_method)
        main_donut_df = rec_raw.groupby('Group')['Count'].sum().reset_index()

    # IUCN Data Processing
    iucn_order = ['CR', 'EN', 'VU', 'DD', 'LC']
    if 'iucn_red_list_category' in df_clean:
        iucn_sightings = df_clean['iucn_red_list_category'].value_counts().reindex(iucn_order).fillna(0).reset_index()
        iucn_sightings.columns = ['Category', 'Sightings']

        iucn_species = df_clean.groupby('iucn_red_list_category')['species_display'].nunique().reindex(iucn_order).fillna(0).reset_index()
        iucn_species.columns = ['Category', 'SpeciesCount']
    else:
        iucn_sightings = pd.DataFrame()
        iucn_species = pd.DataFrame()

    # COLUMN 1 (LEFT 1/3): Waffle Chart for Observation Methodology (No Caption)
    with sec3_col1:
        st.markdown("**Observation Methodology**")
        
        if not rec_raw.empty:
            total_rec = main_donut_df['Count'].sum()
            main_donut_df['Pct'] = (main_donut_df['Count'] / total_rec) * 100
            
            counts_map = dict(zip(main_donut_df['Group'], main_donut_df['Count']))
            pct_map = dict(zip(main_donut_df['Group'], main_donut_df['Pct']))
            
            human_pct = pct_map.get('Human Observation', 0)
            machine_pct = pct_map.get('Machine Observation', 0)
            others_pct = pct_map.get('Others', 0)
            
            n_human = int(round(human_pct))
            n_others_main = max(1, int(round(others_pct))) if others_pct > 0 else 0
            n_machine = 100 - n_human - n_others_main
            
            color_map = {
                'Human Observation': '#0284c7',   # Ocean Blue
                'Machine Observation': '#38bdf8', # Sky Blue
                'Others': '#64748b'              # Slate Grey
            }
            
            shapes = []
            hover_x = []
            hover_y = []
            hover_text = []
            square_size = 0.90
            
            for i in range(100):
                row = 9 - (i // 10)  # Top row 9 down to 0
                col = i % 10         # Left col 0 to 9
                
                if i < n_human:
                    grp = 'Human Observation'
                elif i < n_human + n_machine:
                    grp = 'Machine Observation'
                else:
                    grp = 'Others'
                    
                c = color_map[grp]
                x0 = col
                x1 = col + square_size
                y0 = row
                y1 = row + square_size
                
                shapes.append(dict(
                    type="rect",
                    x0=x0, y0=y0, x1=x1, y1=y1,
                    fillcolor=c,
                    line=dict(width=0),
                    layer="below"
                ))
                
                hover_x.append(x0 + square_size / 2)
                hover_y.append(y0 + square_size / 2)
                cnt = counts_map.get(grp, 0)
                pct = pct_map.get(grp, 0)
                
                if grp == 'Others':
                    hover_text.append(f"<b>{grp}</b><br>Preserved Specimen, Material Sample, etc.<br>Sightings: {cnt:,} ({pct:.1f}%)<br>1 square = 1%")
                else:
                    hover_text.append(f"<b>{grp}</b><br>Sightings: {cnt:,} ({pct:.1f}%)<br>1 square = 1%")

            fig_waffle = go.Figure()
            fig_waffle.add_trace(go.Scatter(
                x=hover_x,
                y=hover_y,
                mode='markers',
                marker=dict(size=22, opacity=0),
                text=hover_text,
                hovertemplate="%{text}<extra></extra>",
                showlegend=False
            ))
            
            # Direct side labels matching reference image style
            annotations = [
                dict(
                    x=-0.5, y=5.8,
                    text=f"<b>{human_pct:.0f}%</b><br>Human Obs",
                    showarrow=False,
                    xanchor="right", yanchor="middle",
                    font=dict(size=11, color=color_map['Human Observation'], family="sans-serif"),
                    align="right"
                ),
                dict(
                    x=-0.5, y=1.2,
                    text=f"<b>{machine_pct:.0f}%</b><br>Machine Obs",
                    showarrow=False,
                    xanchor="right", yanchor="middle",
                    font=dict(size=11, color=color_map['Machine Observation'], family="sans-serif"),
                    align="right"
                ),
                dict(
                    x=10.5, y=0.45,
                    text=f"<b>{others_pct:.1f}%</b><br>Others",
                    showarrow=False,
                    xanchor="left", yanchor="middle",
                    font=dict(size=10, color=color_map['Others'], family="sans-serif"),
                    align="left"
                )
            ]
            
            fig_waffle.update_layout(
                shapes=shapes,
                annotations=annotations,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-4.8, 13.5], fixedrange=True),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-0.5, 10.5], fixedrange=True, scaleanchor="x", scaleratio=1),
                height=350,
                margin=dict(l=0, r=0, t=10, b=10),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_waffle, use_container_width=True)

    # COLUMN 2 (MIDDLE 1/3): Bi-directional Single Bar Plot for IUCN Status
    with sec3_col2:
        st.markdown("**IUCN Red List Status**")
        
        if not iucn_sightings.empty and not iucn_species.empty:
            tot_sightings = iucn_sightings['Sightings'].sum()
            tot_species = iucn_species['SpeciesCount'].sum()
            
            iucn_sightings['Pct'] = (iucn_sightings['Sightings'] / tot_sightings) * 100
            iucn_species['Pct'] = (iucn_species['SpeciesCount'] / tot_species) * 100
            
            fig_iucn_bidir = go.Figure()
            
            # Top bars: Sightings (% share going UP)
            fig_iucn_bidir.add_trace(go.Bar(
                x=iucn_sightings['Category'],
                y=iucn_sightings['Pct'],
                name="Sightings (% of Total)",
                marker_color='#0284c7',
                text=[f"<b>{row['Sightings']:,}</b><br>({row['Pct']:.1f}%)" for _, row in iucn_sightings.iterrows()],
                textposition='outside',
                customdata=np.column_stack((iucn_sightings['Sightings'], iucn_sightings['Pct'])),
                hovertemplate="<b>IUCN Category: %{x}</b><br>Sightings: %{customdata[0]:,}<br>Share: %{customdata[1]:.1f}%<extra></extra>"
            ))
            
            # Bottom bars: Unique Species (% share going DOWN)
            fig_iucn_bidir.add_trace(go.Bar(
                x=iucn_species['Category'],
                y=-iucn_species['Pct'],
                name="Unique Species (% of Total)",
                marker_color='#f43f5e',
                text=[f"<b>{int(row['SpeciesCount'])} spp.</b><br>({row['Pct']:.1f}%)" for _, row in iucn_species.iterrows()],
                textposition='outside',
                customdata=np.column_stack((iucn_species['SpeciesCount'], iucn_species['Pct'])),
                hovertemplate="<b>IUCN Category: %{x}</b><br>Unique Species: %{customdata[0]}<br>Share: %{customdata[1]:.1f}%<extra></extra>"
            ))
            
            # Divider line at Y = 0
            fig_iucn_bidir.add_shape(
                type="line",
                x0=-0.5, x1=4.5, y0=0, y1=0,
                line=dict(color="#1e293b", width=1.5)
            )
            
            fig_iucn_bidir.update_layout(
                barmode='relative',
                height=350,
                margin=dict(l=0, r=0, t=10, b=10),
                yaxis=dict(
                    title="Share (%)",
                    tickvals=[-100, -75, -50, -25, 0, 25, 50, 75, 100],
                    ticktext=["100%", "75%", "50%", "25%", "0%", "25%", "50%", "75%", "100%"],
                    showgrid=True,
                    gridcolor="#f1f5f9"
                ),
                xaxis=dict(title="IUCN Category"),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    font=dict(size=10, family="sans-serif")
                ),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_iucn_bidir, use_container_width=True)

    # COLUMN 3 (RIGHT 1/3): IUCN Category Legend Guide
    with sec3_col3:
        st.markdown("**IUCN Category Guide**")
        st.markdown("""
        - **CR** (*Critically Endangered*):
          High risk of extinction in the wild.
          
        - **EN** (*Endangered*):
          High risk of extinction in the wild.
          
        - **VU** (*Vulnerable*):
          High risk of endangerment in the wild.
          
        - **LC** (*Least Concern*):
          Widespread & abundant taxa.
          
        - **DD** (*Data Deficient*):
          Inadequate data to assess risk.
        """)

    # Checkbox for optional table display
    if st.checkbox("Show Methodology & IUCN Data Tables"):
        mt_c1, mt_c2 = st.columns([1, 2])
        with mt_c1:
            st.dataframe(rec_raw[['Group', 'Methodology_Raw', 'Count']].rename(columns={'Methodology_Raw': 'Methodology'}), use_container_width=True)
        with mt_c2:
            iucn_combined = pd.merge(iucn_sightings, iucn_species, on='Category')
            st.dataframe(iucn_combined.rename(columns={'Category': 'IUCN Category', 'SpeciesCount': 'Unique Species'}), use_container_width=True)


# --- PAGE 2: PLOTLY VISUALIZATIONS ---
elif navigation == "Interactive visualizations":
    st.title("Interactive Cetacea Visualizations")
    
    # Global Filters in Sidebar for Page 2
    st.sidebar.markdown("---")
    st.sidebar.subheader("Interactive Filters")

    # Species Filter (Multi-select)
    display_opts = sorted(df_clean['species_display'].unique().tolist())
    selected_sp_display = st.sidebar.multiselect(
        "Select Species (Common / Scientific):",
        display_opts,
        placeholder="Choose one or more species (leave empty for All)"
    )

    # Country Filter using country names
    country_opts = ["All"] + sorted(df_clean['country_name'].dropna().unique().tolist())
    selected_country = st.sidebar.selectbox("Select Country:", country_opts)

    # Year Filter
    if 'occurrence_year' in df_clean:
        all_years = ["All"] + sorted([int(y) for y in df_clean['occurrence_year'].dropna().unique()])
        selected_year = st.sidebar.selectbox("Select Year:", all_years)
    else:
        selected_year = "All"

    # Filter dataset
    filtered_df = df_clean.copy()
    if selected_sp_display:
        filtered_df = filtered_df[filtered_df['species_display'].isin(selected_sp_display)]
    if selected_country != "All":
        filtered_df = filtered_df[filtered_df['country_name'] == selected_country]
    if selected_year != "All":
        filtered_df = filtered_df[filtered_df['occurrence_year'] == selected_year]

    st.markdown(f"Displaying **{len(filtered_df):,}** filtered sightings.")

    # Tabs for Visualizations
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Global Sightings Map",
        "Latitudinal Migration",
        "Effort by Country",
        "Taxonomic Hierarchy",
        "IUCN Status",
        "Monthly Animation"
    ])

    # Tab 1: Global Sightings Point Map
    with tab1:
        st.subheader("Global Cetacea Sightings Map")
        sample_map = filtered_df.dropna(subset=["latitude", "longitude"])
        if len(sample_map) > 20000:
            sample_map = sample_map.sample(20000, random_state=42)
        
        color_col = "family" if 'family' in sample_map and not selected_sp_display else "species_display"
        
        fig_map = px.scatter_map(
            sample_map,
            lat="latitude",
            lon="longitude",
            color=color_col,
            hover_name="species_display",
            zoom=1.2,
            center=dict(lat=20, lon=0),
            height=600
        )
        fig_map.update_traces(marker=dict(size=5, opacity=0.7))
        fig_map.update_layout(
            map_style="open-street-map",
            margin=dict(l=0, r=0, t=30, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=-0.18, xanchor="center", x=0.5)
        )
        st.plotly_chart(fig_map, use_container_width=True)

    # Tab 2: Latitudinal Migration Drift
    with tab2:
        st.subheader("Monthly Latitudinal Migration Wave")
        top_7_species = filtered_df['common_name'].value_counts().head(7).index
        df_top7 = filtered_df[filtered_df['common_name'].isin(top_7_species)]
        
        if not df_top7.empty and 'occurrence_month' in df_top7:
            monthly_lat = df_top7.groupby(['common_name', 'occurrence_month'])['latitude'].median().reset_index()
            month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            
            fig_migration = go.Figure()
            for sp in top_7_species:
                df_sp = monthly_lat[monthly_lat['common_name'] == sp]
                fig_migration.add_trace(
                    go.Scatter(
                        x=df_sp['occurrence_month'],
                        y=df_sp['latitude'],
                        mode='lines+markers',
                        name=sp,
                        line=dict(width=3, shape='spline'),
                        marker=dict(size=7)
                    )
                )
            fig_migration.update_layout(
                title="Median Latitudinal Migration by Month (Top Species)",
                xaxis=dict(title="Month of Year", tickmode="array", tickvals=list(range(1, 13)), ticktext=month_names),
                yaxis=dict(title="Median Latitude (°N / °S)", zeroline=True),
                legend_title_text="Species",
                height=550
            )
            st.plotly_chart(fig_migration, use_container_width=True)
        else:
            st.info("Insufficient data to generate latitudinal migration curve for current selection.")

    # Tab 3: Observer Effort & Methodology Bias
    with tab3:
        st.subheader("Observer Effort & Methodology Bias Across Top Countries")
        top_10_countries = filtered_df['country_name'].value_counts().head(10).index
        df_top_c = filtered_df[filtered_df['country_name'].isin(top_10_countries)]
        
        if not df_top_c.empty and 'basis_of_record' in df_top_c:
            ct = pd.crosstab(df_top_c['country_name'], df_top_c['basis_of_record'], normalize='index') * 100
            main_types = [c for c in ['HUMAN_OBSERVATION', 'MACHINE_OBSERVATION', 'PRESERVED_SPECIMEN'] if c in ct.columns]
            ct = ct[main_types]
            if 'HUMAN_OBSERVATION' in ct.columns:
                ct = ct.sort_values(by='HUMAN_OBSERVATION', ascending=True)

            fig_bias = go.Figure()
            for record_type in main_types:
                fig_bias.add_trace(
                    go.Bar(
                        y=ct.index,
                        x=ct[record_type],
                        name=record_type.replace('_', ' ').title(),
                        orientation='h'
                    )
                )
            fig_bias.update_layout(
                barmode='stack',
                title="Observation Method Percentage Breakdown (Top 10 Countries)",
                xaxis=dict(title="Percentage (%)", range=[0, 100]),
                yaxis=dict(title="Country"),
                height=550
            )
            st.plotly_chart(fig_bias, use_container_width=True)
        else:
            st.info("Insufficient methodological data for current selection.")

    # Tab 4: Taxonomic Sunburst
    with tab4:
        st.subheader("Taxonomic Hierarchy Breakdown: Family ➔ Genus ➔ Ordinary Name")
        df_taxa = filtered_df.dropna(subset=['family_italic', 'genus_italic', 'common_name'])
        if not df_taxa.empty:
            fig_sunburst = px.sunburst(
                df_taxa,
                path=['family_italic', 'genus_italic', 'common_name'],
                color='family_italic',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_sunburst.update_layout(height=650, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_sunburst, use_container_width=True)

    # Tab 5: IUCN Conservation Status
    with tab5:
        st.subheader("Risk Profile & IUCN Red List Status")
        df_iucn = filtered_df.dropna(subset=['iucn_red_list_category'])
        if not df_iucn.empty:
            iucn_colors = {'CR': '#dc2626', 'EN': '#f97316', 'VU': '#f59e0b', 'DD': '#64748b', 'LC': '#10b981'}
            species_iucn = df_iucn.groupby(['common_name', 'iucn_red_list_category']).size().reset_index(name='sighting_count')
            species_iucn = species_iucn.sort_values(by='sighting_count', ascending=True).tail(20)
            
            fig_iucn = px.bar(
                species_iucn,
                x="sighting_count",
                y="common_name",
                color="iucn_red_list_category",
                orientation="h",
                color_discrete_map=iucn_colors,
                labels={"sighting_count": "Sighting Volume", "common_name": "Common Name", "iucn_red_list_category": "IUCN Status"}
            )
            fig_iucn.update_layout(height=600, margin=dict(l=150, r=50, t=40, b=50))
            st.plotly_chart(fig_iucn, use_container_width=True)

    # Tab 6: Animated Migration Map
    with tab6:
        st.subheader("Monthly Cetacea Migration Animation")
        top_species_anim = filtered_df['common_name'].value_counts().head(6).index
        df_anim = filtered_df[filtered_df['common_name'].isin(top_species_anim)].copy()
        
        if not df_anim.empty and 'occurrence_month' in df_anim:
            df_anim = df_anim.sort_values(by='occurrence_month')
            df_anim_sample = df_anim.sample(min(15000, len(df_anim)), random_state=42) if len(df_anim) > 15000 else df_anim
            
            fig_anim_map = px.scatter_map(
                df_anim_sample,
                lat="latitude",
                lon="longitude",
                color="common_name",
                animation_frame="occurrence_month",
                category_orders={"occurrence_month": list(range(1, 13))},
                hover_name="common_name",
                map_style="open-street-map",
                center=dict(lat=20, lon=0),
                zoom=1.2,
                opacity=0.7,
                title="Animated Global Migration Across 12 Months"
            )
            fig_anim_map.update_traces(marker=dict(size=5))
            fig_anim_map.update_layout(height=600, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_anim_map, use_container_width=True)


# --- PAGE 3: DATA EXPLORER ---
elif navigation == "Data explorer":
    st.title("Interactive Data Explorer")
    st.write("Filter, inspect, and export the complete cetacea biodiversity dataset.")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        sp_filter = st.multiselect("Filter by Common / Scientific Name:", options=sorted(df_clean['species_display'].unique()))
    with col_f2:
        country_filter = st.multiselect("Filter by Country:", options=sorted(df_clean['country_name'].dropna().unique()))
    with col_f3:
        if 'iucn_red_list_category' in df_clean:
            iucn_opts = [x for x in df_clean['iucn_red_list_category'].unique() if pd.notna(x)]
            iucn_filter = st.multiselect("Filter by IUCN Status:", options=iucn_opts)
        else:
            iucn_filter = []

    exp_df = df_clean.copy()
    if sp_filter:
        exp_df = exp_df[exp_df['species_display'].isin(sp_filter)]
    if country_filter:
        exp_df = exp_df[exp_df['country_name'].isin(country_filter)]
    if iucn_filter and 'iucn_red_list_category' in exp_df:
        exp_df = exp_df[exp_df['iucn_red_list_category'].isin(iucn_filter)]

    # Display columns nicely with Unicode italic scientific and family names
    display_cols = ['common_name', 'scientific_italic', 'family_italic', 'genus_italic', 'latitude', 'longitude', 'country_name', 'occurrence_year', 'basis_of_record', 'iucn_red_list_category']
    display_cols = [c for c in display_cols if c in exp_df.columns]
    
    view_df = exp_df[display_cols].rename(columns={'common_name': 'Common Name', 'scientific_italic': 'Scientific Name', 'family_italic': 'Family', 'genus_italic': 'Genus', 'country_name': 'Country'})
    st.dataframe(view_df, use_container_width=True)

    # Export CSV Button for full filtered dataset
    csv = exp_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Filtered Dataset (CSV)",
        data=csv,
        file_name="cetacea_biodiversity_filtered.csv",
        mime="text/csv"
    )
