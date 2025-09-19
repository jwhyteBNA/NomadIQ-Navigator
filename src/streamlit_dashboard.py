import requests
import pandas as pd
import numpy as np
import altair as alt
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

API_URL = "http://localhost:8000"

st.set_page_config(page_title="NomadIQ Website Mockup", layout="wide")

def get_park_usage_summarized(park_name=None, aggregate=False, year=None):
    """Fetch summarized usage data for all parks or a specific park, optionally filtered by year."""
    params = {"granularity": "annual"}
    if aggregate:
        params["aggregate"] = True
    if park_name:
        params["park_name"] = park_name
    if year:
        params["year"] = year
    resp = requests.get(f"{API_URL}/parks/usage", params=params)
    if resp.status_code == 200:
        return resp.json()
    return []

def fetch_all_parks():
    resp = requests.get(f"{API_URL}/parks")
    if resp.status_code == 200:
        df = pd.DataFrame(resp.json())
        return ["All Parks"] + sorted(df["name"].tolist())
    return ["All Parks"]

all_parks = fetch_all_parks()
selected_park = st.sidebar.selectbox(
    "Select a National Park",
    all_parks,
    index=0
)


selected_parks = None
if selected_park == "All Parks":
    selected_parks = st.sidebar.multiselect(
        "Filter Annual Visit Data By Park", all_parks[1:], default=all_parks[1:3])


st.title(f"NomadIQ Navigator Dashboard - {selected_park}")

def get_all_parks():
    resp = requests.get(f"{API_URL}/parks")
    if resp.status_code == 200:
        return pd.DataFrame(resp.json())
    return pd.DataFrame()

parks_df = get_all_parks()
if not parks_df.empty:
    st.subheader("National Parks Map")
    if selected_park == "All Parks":
        parks_df["highlight"] = False
        parks_df["highlight_label"] = "All Parks"
        fig = px.scatter_map(
        parks_df,
        lat="latitude",
        lon="longitude",
        hover_name="name",
        color="highlight_label",
        size=[10]*len(parks_df),
        zoom=3,
        center={"lat": 39.8283, "lon": -98.5795},
        height=600
    )
        fig.update_layout(showlegend=False)
    else:
        parks_df["highlight"] = parks_df["name"].str.lower() == selected_park.lower()
        parks_df["highlight_label"] = parks_df["highlight"].replace({True: "Selected Park", False: "All Parks"})
        fig = px.scatter_map(
            parks_df,
            lat="latitude",
            lon="longitude",
            hover_name="name",
            color="highlight_label",
            size=parks_df["highlight"].replace({True: 20, False: 10}),
            zoom=3,
            center={"lat": 39.8283, "lon": -98.5795},
            height=600
        )
        fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    # Profile below map
    if selected_park == "All Parks":
        st.subheader("All Parks Profile Table")
        st.dataframe(parks_df)
    else:
        park_row = parks_df[parks_df["name"].str.lower() == selected_park.lower()]
        if not park_row.empty:
            park_info = park_row.iloc[0]
            st.subheader(f"{selected_park} Profile")
            col1, col2, col3 = st.columns(3)
            col1.metric("Park Code", park_info.get("park_code", "N/A"))
            col2.metric("State(s)", park_info.get("states", "N/A"))
            col3.metric("Designation", park_info.get("designation", "N/A"))

            col4, col5, col6 = st.columns(3)
            col4.metric("Latitude", f"{park_info.get('latitude', 'N/A')}")
            col5.metric("Longitude", f"{park_info.get('longitude', 'N/A')}")
            url = park_info.get("url", None)
            if url:
                col6.markdown(f"[Park Website]({url})")
            else:
                col6.markdown("No website available")

            st.markdown(f"**Description:** {park_info.get('description', 'N/A')}")

            st.markdown(f"**Address:** {park_info.get('address_line1', '')} {park_info.get('address_line2', '')}, {park_info.get('address_city', '')}, {park_info.get('address_state', '')} {park_info.get('address_zip', '')}")
            st.markdown(f"**Email:** {park_info.get('email', 'N/A')}")
            st.markdown(f"**Phone:** {park_info.get('phone_number', 'N/A')}{' ext. ' + str(park_info.get('phone_extension')) if park_info.get('phone_extension') else ''}")

            entrance_fees = park_info.get('entrance_fees', 'N/A')
            if entrance_fees and entrance_fees != 'N/A':
                fee_items = [fee.strip() for fee in entrance_fees.split(',') if fee.strip()]
                st.markdown("**Entrance Fees:**")
                for fee in fee_items:
                    st.markdown(f"- {fee}")
            else:
                st.markdown("**Entrance Fees:** N/A")
            st.markdown(f"**Annual Pass Fee:** {park_info.get('annual_pass_fee', 'N/A')}")
            st.markdown(f"**Annual Pass Description:** {park_info.get('annual_pass_description', 'N/A')}")

            st.markdown(f"**Activities:** {park_info.get('activities', 'N/A')}")
            st.markdown(f"**Park Themes:** {park_info.get('park_themes', 'N/A')}")

            starting_park = park_info.get("name", None)
            if starting_park:
                try:
                    response = requests.get(f"http://localhost:8000/parks/distances?starting_national_park={starting_park.lower()}")
                    if response.status_code == 200:
                        distances_data = response.json()
                        if distances_data:
                            dist_df = pd.DataFrame(distances_data)
                            dist_df = dist_df.sort_values("distance_miles", ascending=True)
                            st.subheader(f"Distances from {starting_park} to Other Parks")
                            chart = alt.Chart(dist_df).mark_bar().encode(
                                x=alt.X('distance_miles:Q', title='Distance (miles)'),
                                y=alt.Y('destination_national_park:N', sort=list(dist_df['destination_national_park']), title='Destination Park'),
                                tooltip=['destination_national_park', 'distance_miles']
                            ).properties(height=300)
                            st.altair_chart(chart, use_container_width=True)
                        else:
                            st.info("No distance data available for this park.")
                    else:
                        st.warning("Could not fetch distance data.")
                except Exception as e:
                    st.error(f"Error fetching distance data: {e}")


def get_park_profile(name=None):
	if name is None or name == "All Parks":
		resp = requests.get(f"{API_URL}/parks")
	else:
		resp = requests.get(f"{API_URL}/parks", params={"name": name})
	if resp.status_code == 200:
		return resp.json()
	return {}

profile = get_park_profile(selected_park)


def get_rec_visitor_data(name=None, year=None, month=None):
    params = {"granularity": "monthly"}
    if name is not None and name != "All Parks":
        params["park_name"] = name
    if year is not None:
        params["year"] = year
    if month is not None:
        params["month"] = month
    resp = requests.get(f"{API_URL}/parks/usage", params=params)
    if resp.status_code == 200:
        return resp.json()
    return []


def get_park_alerts(park_name=None):
	if park_name is None or park_name == "All Parks":
		resp = requests.get(f"{API_URL}/parks/alerts")
	else:
		resp = requests.get(f"{API_URL}/parks/alerts", params={"park_name": park_name})
	if resp.status_code == 200:
		return resp.json()
	return []

alerts = get_park_alerts(selected_park)
if isinstance(alerts, list) and alerts:
    filtered_alerts = [a for a in alerts if a.get("alert_title") not in [None, ""] or a.get("alert_category") not in [None, ""]]
    alerts_sorted = sorted(
        filtered_alerts,
        key=lambda x: str(x.get("lastIndexedDate") or ""),
        reverse=True
    )
    df_alerts = pd.DataFrame(alerts_sorted)
    if not df_alerts.empty and "alert_category" in df_alerts.columns:
        st.subheader("Park Alerts by Category")
        df_alerts["alert_category"] = df_alerts["alert_category"].fillna("")
        df_alerts["alert_category"] = df_alerts["alert_category"].apply(lambda x: x if x in ["Park Closure", "Danger", "Road Closure", "Information"] else "Other")
        category_order = ["Park Closure", "Danger", "Road Closure", "Information", "Other"]
        category_colors = {
            "Park Closure": "#d62728", 
            "Danger": "#ff7f0e",       
            "Road Closure": "#ffeb3b", 
            "Information": "#2ca02c",  
            "Other": "#800080"          
        }
        alert_counts = df_alerts["alert_category"].value_counts().reset_index()
        alert_counts.columns = ["alert_category", "count"]
        merged = pd.merge(alert_counts, df_alerts, on="alert_category", how="left")
        merged["alert_category"] = pd.Categorical(merged["alert_category"], categories=category_order, ordered=True)
        chart_alerts = alt.Chart(merged).mark_bar().encode(
            x=alt.X("alert_category:N", title="Alert Category", sort=category_order),
            y=alt.Y("count:Q", title="Count"),
            color=alt.Color("alert_category:N", scale=alt.Scale(domain=list(category_colors.keys()), range=list(category_colors.values())), legend=None),
            tooltip=["park_name", "alert_title", "alert_description", "alert_url","lastIndexedDate"]
        ).properties(
            width=700,
            height=400,
            title="Number of Alerts by Category"
        )
        st.altair_chart(chart_alerts, use_container_width=True)
    if selected_park != "All Parks":
        st.subheader("Park Alerts")
        st.dataframe(df_alerts)
else:
    st.info("No alerts available for this selection - all operations normal.")


# --- Monthly Recreation Chart ---
def show_monthly_recreation_chart(selected_parks):
    all_monthly_data = get_rec_visitor_data(None)
    df_all_monthly = pd.DataFrame(all_monthly_data)
    if not df_all_monthly.empty and all(col in df_all_monthly.columns for col in ["Year", "Month", "RecreationVisits", "park_name"]):
        df_all_monthly = df_all_monthly[df_all_monthly["Year"].notnull() & df_all_monthly["Month"].notnull()]
        df_all_monthly = df_all_monthly[(df_all_monthly["Year"] != 0) & (df_all_monthly["Month"] != 0)]
        df_all_monthly = df_all_monthly[df_all_monthly["Year"].apply(lambda x: pd.notna(x) and np.isfinite(x))]
        df_all_monthly = df_all_monthly[df_all_monthly["Month"].apply(lambda x: pd.notna(x) and np.isfinite(x))]
        df_all_monthly["Month"] = df_all_monthly["Month"].astype(int)
        df_all_monthly["Year"] = df_all_monthly["Year"].astype(int)
        if not selected_parks or len(selected_parks) == 0:
            parks_to_show = df_all_monthly["park_name"].unique().tolist()
        else:
            parks_to_show = selected_parks
        df_all_monthly = df_all_monthly[df_all_monthly["park_name"].isin(parks_to_show)]
        df_all_monthly = df_all_monthly[df_all_monthly["Month"].between(1,12)]
        df_all_monthly.sort_values(["park_name", "Month"], inplace=True)
        fig = px.line(
            df_all_monthly,
            x="Month",
            y="RecreationVisits",
            color="Year",
            line_group="park_name",
            markers=True,
            labels={
                "Month": "Month",
                "RecreationVisits": "Recreation Visits",
                "Year": "Year",
                "park_name": "Park"
            },
            title="Monthly Recreational Visits by Park (Year Colored)"
        )
        fig.update_layout(xaxis=dict(tickmode='array', tickvals=list(range(1,13)), ticktext=[str(m) for m in range(1,13)]), height=500, width=900)
        import uuid
        chart_key = f"monthly_recreation_chart_{'_'.join([str(p) for p in parks_to_show])}_{uuid.uuid4()}"
        st.subheader("Park Attendance Trends")
        st.plotly_chart(fig, use_container_width=True, key=chart_key)
    else:
        st.warning("No valid monthly recreation data available.")



if selected_park == "All Parks":
    parks_list = selected_parks if selected_parks else [p for p in all_parks if p != "All Parks"]
else:
    parks_list = [selected_park]
show_monthly_recreation_chart(parks_list)
# --- Annual Totals for All Parks ---
agg_usage_data = get_park_usage_summarized(aggregate=True)
if agg_usage_data:
    df_agg = pd.DataFrame(agg_usage_data)
    df_agg = df_agg[df_agg["Year"] != 0]
    usage_melt = df_agg.melt(
        id_vars=["Year"],
        value_vars=[
            "total_recreation_visits",
            "total_non_recreation_visits",
            "total_concessioner_camping",
            "total_tent_campers",
            "total_rv_campers"
        ],
        var_name="VisitType",
        value_name="Visits"
    )
    total_per_year = usage_melt.groupby("Year")["Visits"].transform("sum")
    usage_melt["Percent"] = (usage_melt["Visits"] / total_per_year) * 100
    chart_agg = alt.Chart(usage_melt).mark_bar().encode(
        x=alt.X("Year:N", title="Year", sort="-y"),
        y=alt.Y("Percent:Q", title="Percent of Total Visits", stack="zero"),
        color=alt.Color("VisitType:N", title="Metric"),
        tooltip=["Year", "VisitType", "Visits", "Percent"]
    ).properties(
        width=900,
        height=500,
        title="Proportional Annual Totals for All Parks"
    )
    st.altair_chart(chart_agg, use_container_width=True)
else:
    st.info("No aggregated usage data available.")


# --- National Parks Heatmap ---
if 'parks_df' in locals() or 'parks_df' in globals():
    parks_states = parks_df.copy()
    parks_states = parks_states.assign(state=parks_states['states'].str.split(',')).explode('state')
    parks_states['state'] = parks_states['state'].str.strip()
    park_counts = parks_states['state'].value_counts().reset_index()
    park_counts.columns = ['state', 'num_parks']
    fig = px.choropleth(
        park_counts,
        locations='state',
        locationmode='USA-states',
        color='num_parks',
        scope='usa',
        color_continuous_scale='Blues',
        labels={'num_parks': 'National Parks'},
        title='Number of National Parks by State'
    )
    st.subheader("National Parks by State (Heatmap)")
    st.plotly_chart(fig, use_container_width=True)


def get_parks_to_landmarks(park_name=None, state_abbr=None, limit=100, offset=0):
    params = {}
    if park_name:
        params["park_name"] = park_name
    if state_abbr:
        params["landmark_state"] = state_abbr
    params["limit"] = limit
    params["offset"] = offset
    resp = requests.get(f"{API_URL}/parks/landmarks", params=params)
    if resp.status_code == 200:
        return resp.json()
    return []

st.subheader("Parks to Nearby Landmarks")
col_landmark1, col_landmark2 = st.columns(2)
park_filter = col_landmark1.text_input("Filter by Park Name")
state_filter = col_landmark2.text_input("Filter by State")

landmarks_data = get_parks_to_landmarks(
    park_name=park_filter if park_filter else None,
    state_abbr=state_filter if state_filter else None,
    limit=100,
    offset=0
)
if landmarks_data:
    df_landmarks = pd.DataFrame(landmarks_data)
    st.dataframe(df_landmarks)


def get_landmarks_summary(state=None, state_abbr=None):
    params = {}
    if state:
        params["state"] = state
    if state_abbr:
        params["state_abbr"] = state_abbr
    resp = requests.get(f"{API_URL}/landmarks/summary", params=params)
    if resp.status_code == 200:
        return resp.json()
    return {}

landmarks_response = requests.get(f"{API_URL}/landmarks")
landmarks = pd.DataFrame(landmarks_response.json()) if landmarks_response.status_code == 200 else pd.DataFrame()

# Treemap visualization
if not landmarks.empty and 'state_abbr' in landmarks.columns and 'level_of_significance' in landmarks.columns:
    lm_valid = landmarks.dropna(subset=['state_abbr', 'level_of_significance'])
    if not lm_valid.empty:
        st.subheader("National Register of Historic Landmarks Details")
        fig_treemap = px.treemap(
            lm_valid,
            path=['state_abbr', 'level_of_significance'],
            values=None,  
            color='level_of_significance',
            title= "Landmarks by State and Level of Significance"
        )
        fig_treemap.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_treemap, use_container_width=True)
    else:
        st.info("No valid state and level_of_significance data for treemap.")
else:
    st.info("State or level_of_significance data not available for landmarks.")


if not landmarks.empty and 'state_abbr' in landmarks.columns and 'category_of_property' in landmarks.columns:
    lm_valid = landmarks.dropna(subset=['state_abbr', 'category_of_property'])
    if not lm_valid.empty:
        fig_treemap = px.treemap(
            lm_valid,
            path=['state_abbr', 'category_of_property'],
            values=None,  
            color='category_of_property',
            title= "Landmarks by State and Category of Property"
        )
        fig_treemap.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_treemap, use_container_width=True)
    else:
        st.info("No valid state and category_of_property data for treemap.")
else:
    st.info("State or category_of_property data not available for landmarks.")

selected_state_abbr = None
if selected_park != "All Parks":
    park_row = parks_df[parks_df["name"].str.lower() == selected_park.lower()]
    if not park_row.empty:
        selected_state_abbr = park_row.iloc[0]["states"].split(",")[0].strip().upper()

if selected_park == "All Parks":
    summary_data = get_landmarks_summary()
else:
    summary_data = get_landmarks_summary(state_abbr=selected_state_abbr)


def get_state_parks_near_national_park(national_park_name):
    resp = requests.get(f"{API_URL}/parks/state-distances", params={"national_park_name": national_park_name})
    if resp.status_code == 200:
        return resp.json()
    return []

activity_icons = {
    "camping_available": "🏕️",
    "boating_available": "🚤",
    "biking_hiking_available": "🚴",
    "fishing_available": "🎣",
    "golf_available": "⛳",
    "equestrian_available": "🐎",
    "ohv_available": "🏍️",
    "winter_recreation_available": "❄️",
    "wildlife_available": "🦌"
}

def format_activities(row):
    return " ".join([icon for key, icon in activity_icons.items() if row.get(key)])


# --- State Parks Map Visualization ---
if selected_park != "All Parks":
    state_parks_data = get_state_parks_near_national_park(selected_park)
    if state_parks_data and isinstance(state_parks_data, list) and len(state_parks_data) > 0:
        df_state_parks = pd.DataFrame(state_parks_data)
        st.subheader(f"Utah State Parks Near {selected_park}")
        fig = go.Figure()
        np_lat = df_state_parks.iloc[0]['national_park_latitude']
        np_lon = df_state_parks.iloc[0]['national_park_longitude']
        if pd.isnull(np_lat) or pd.isnull(np_lon):
            np_lat = 39.8283
            np_lon = -98.5795
        fig.add_trace(go.Scattermap(
            lat=[np_lat],
            lon=[np_lon],
            mode='markers',
            marker=dict(size=16, color='blue'),
            name=selected_park,
            text=[selected_park],
        ))
        for _, row in df_state_parks.iterrows():
            activities = format_activities(row) 
            hover_text = (
                f"{row['state_park_name']} ({row['distance_miles']} mi)<br>"
                f"{row['state_park_address']}, {row['state_park_city']}, {row['state_park_zip']}<br>"
                f"Activities: {activities}"
            )
            fig.add_trace(go.Scattermap(
                lat=[row['state_park_latitude']],
                lon=[row['state_park_longitude']],
                mode='markers',
                marker=dict(size=12, color='green'),
                name=row['state_park_name'],
                text=[hover_text]
            ))
        fig.update_layout(
            mapbox=dict(
                style="open-street-map",
                center=dict(lat=39.8283, lon=-98.5795),
                zoom=9
            ),
            margin={"r":0,"t":0,"l":0,"b":0},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No matching state parks found for this national park.")