import requests
import pandas as pd
import numpy as np
import altair as alt
import streamlit as st
import plotly.express as px

API_URL = "http://localhost:8000"


def fetch_all_parks():
    resp = requests.get(f"{API_URL}/park_profile")
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
        "Filter Parks for Recreation Chart", all_parks[1:], default=all_parks[1:3])


st.title(f"NomadIQ Navigator Dashboard - {selected_park}")

def get_all_parks():
    resp = requests.get(f"{API_URL}/park_profile")
    if resp.status_code == 200:
        return pd.DataFrame(resp.json())
    return pd.DataFrame()

parks_df = get_all_parks()
if not parks_df.empty:
    # Move map above profile table
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
            # Card-style columns for key info
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

            # Format entrance fees as a bulleted list
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


            # Only show chart if a single park is selected
            starting_park = park_info.get("name", None)
            if starting_park:
                try:
                    response = requests.get(f"http://localhost:8000/nps_distances?starting_national_park={starting_park.lower()}")
                    if response.status_code == 200:
                        distances_data = response.json()
                        if distances_data:
                            dist_df = pd.DataFrame(distances_data)
                            # Sort by distance ascending (closest parks first)
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
		resp = requests.get(f"{API_URL}/park_profile")
	else:
		resp = requests.get(f"{API_URL}/park_profile", params={"name": name})
	if resp.status_code == 200:
		return resp.json()
	return {}

profile = get_park_profile(selected_park)


def get_rec_visitor_data(name=None):
	if name is None or name == "All Parks":
		resp = requests.get(f"{API_URL}/nps_park_usage_annual")
	else:
		resp = requests.get(f"{API_URL}/nps_park_usage_annual", params={"name": name})
	if resp.status_code == 200:
		return resp.json()
	return []


def get_park_alerts(park_name=None):
	if park_name is None or park_name == "All Parks":
		resp = requests.get(f"{API_URL}/park_alerts")
	else:
		resp = requests.get(f"{API_URL}/park_alerts", params={"park_name": park_name})
	if resp.status_code == 200:
		return resp.json()
	return []

alerts = get_park_alerts(selected_park)
if isinstance(alerts, list) and alerts:
    # Filter out alerts where both alert_title and alert_category are null/empty
    filtered_alerts = [a for a in alerts if a.get("alert_title") not in [None, ""] or a.get("alert_category") not in [None, ""]]
    alerts_sorted = sorted(
        filtered_alerts,
        key=lambda x: str(x.get("lastIndexedDate") or ""),
        reverse=True
    )
    df_alerts = pd.DataFrame(alerts_sorted)
    # Bar chart for alert categories
    if not df_alerts.empty and "alert_category" in df_alerts.columns:
        st.subheader("Park Alerts by Category")
        # Map unknown/empty/null categories to 'Other'
        df_alerts["alert_category"] = df_alerts["alert_category"].fillna("")
        df_alerts["alert_category"] = df_alerts["alert_category"].apply(lambda x: x if x in ["Park Closure", "Danger", "Road Closure", "Information"] else "Other")
        # Define custom order and colors
        category_order = ["Park Closure", "Danger", "Road Closure", "Information", "Other"]
        category_colors = {
            "Park Closure": "#d62728",  # Red
            "Danger": "#ff7f0e",       # Orange
            "Road Closure": "#ffeb3b", # Yellow
            "Information": "#2ca02c",  # Green
            "Other": "#800080"          # Purple
        }
        alert_counts = df_alerts["alert_category"].value_counts().reset_index()
        alert_counts.columns = ["alert_category", "count"]
        # Merge to get details for tooltips
        merged = pd.merge(alert_counts, df_alerts, on="alert_category", how="left")
        # Set categorical order
        merged["alert_category"] = pd.Categorical(merged["alert_category"], categories=category_order, ordered=True)
        chart_alerts = alt.Chart(merged).mark_bar().encode(
            x=alt.X("alert_category:N", title="Alert Category", sort=category_order),
            y=alt.Y("count:Q", title="Count"),
            color=alt.Color("alert_category:N", scale=alt.Scale(domain=list(category_colors.keys()), range=list(category_colors.values())), legend=None),
            tooltip=["park_name", "alert_title", "alert_description", "lastIndexedDate"]
        ).properties(
            width=700,
            height=400,
            title="Number of Alerts by Category"
        )
        st.altair_chart(chart_alerts, use_container_width=True)
    # ...existing code for alert table and info...
    if selected_park == "All Parks":
        st.subheader("Alerts for All Parks")
        st.dataframe(df_alerts)
    else:
        st.subheader("Park Alerts")
        st.dataframe(df_alerts)
else:
    st.info("No alerts available for this selection - all operations normal.")


# --- Function for Plotly Monthly Recreation Chart ---
def show_monthly_recreation_chart(selected_parks):
    # Fetch all parks' monthly recreation data in one API call
    all_monthly_data = get_rec_visitor_data(None)
    df_all_monthly = pd.DataFrame(all_monthly_data)
    # Only keep rows with valid month/year and recreation visits
    if not df_all_monthly.empty and all(col in df_all_monthly.columns for col in ["Year", "Month", "RecreationVisits", "park_name"]):
        df_all_monthly = df_all_monthly[df_all_monthly["Year"].notnull() & df_all_monthly["Month"].notnull()]
        df_all_monthly = df_all_monthly[(df_all_monthly["Year"] != 0) & (df_all_monthly["Month"] != 0)]
        df_all_monthly = df_all_monthly[df_all_monthly["Year"].apply(lambda x: pd.notna(x) and np.isfinite(x))]
        df_all_monthly = df_all_monthly[df_all_monthly["Month"].apply(lambda x: pd.notna(x) and np.isfinite(x))]
        df_all_monthly["Month"] = df_all_monthly["Month"].astype(int)
        # Filter parks client-side
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
            color="park_name",
            markers=True,
            labels={
                "Month": "Month",
                "RecreationVisits": "Recreation Visits",
                "park_name": "Park"
            },
            title="Monthly Recreational Visits by Park"
        )
        fig.update_layout(xaxis=dict(tickmode='array', tickvals=list(range(1,13)), ticktext=[str(m) for m in range(1,13)]), height=500, width=900)
        import uuid
        chart_key = f"monthly_recreation_chart_{'_'.join([str(p) for p in parks_to_show])}_{uuid.uuid4()}"
        st.subheader("Monthly Recreational Visits (Plotly)")
        st.plotly_chart(fig, use_container_width=True, key=chart_key)
    else:
        st.warning("No valid monthly recreation data available.")

# Ensure the monthly recreation chart is rendered immediately after sidebar selection

# Only show the chart once, using sidebar logic
if selected_park == "All Parks":
    # If sidebar filter is empty, show all parks
    if selected_parks is None or len(selected_parks) == 0:
        # Remove "All Parks" from the list
        parks_list = [p for p in all_parks if p != "All Parks"]
        show_monthly_recreation_chart(parks_list)
    else:
        show_monthly_recreation_chart(selected_parks)
else:
    show_monthly_recreation_chart([selected_park])


def get_park_usage_summarized(park_name=None, aggregate=False):
# --- Park Profile Card (after all function definitions) ---
    profile = get_park_profile(selected_park)
    if selected_park == "All Parks":
        if profile:
            st.subheader("All Parks Profiles")
            st.dataframe(profile)
    elif profile:
        st.subheader(f"{selected_park} Profile")
        # Assume profile is a dict or single-row DataFrame
        if isinstance(profile, dict):
            park_info = profile
        elif isinstance(profile, list) and len(profile) > 0:
            park_info = profile[0]
        elif isinstance(profile, pd.DataFrame):
            park_info = profile.iloc[0].to_dict()
        else:
            park_info = profile

        # Fetch latest year’s usage summary for selected park
        usage_data = get_park_usage_summarized(selected_park)
        latest_year = None
        if usage_data is not None and isinstance(usage_data, (list, pd.DataFrame)):
            df_usage = pd.DataFrame(usage_data)
            if not df_usage.empty and "Year" in df_usage.columns:
                latest_year = df_usage["Year"].max()
                latest_row = df_usage[df_usage["Year"] == latest_year].iloc[0]
                annual_visitors = int(latest_row.get("total_recreation_visits", 0))
                tent_campers = int(latest_row.get("total_tent_campers", 0))
                rv_campers = int(latest_row.get("total_rv_campers", 0))
            else:
                annual_visitors = tent_campers = rv_campers = 0
        else:
            annual_visitors = tent_campers = rv_campers = 0

        # Card-style layout
        st.markdown(f"### {park_info.get('name', selected_park)} 🏞️")
        st.markdown(f"**State(s):** {park_info.get('states', 'N/A')}")
        st.markdown(f"**Designation:** {park_info.get('designation', 'N/A')}")
        st.markdown(f"**Area:** {park_info.get('area', 'N/A')} acres")
        st.markdown(f"**Latitude/Longitude:** {park_info.get('latitude', 'N/A')}, {park_info.get('longitude', 'N/A')}")

        # Display metrics in columns
        col1, col2, col3 = st.columns(3)
        col1.metric(f"Annual Visitors ({latest_year if latest_year else ''})", f"{annual_visitors:,}")
        col2.metric(f"Tent Campers ({latest_year if latest_year else ''})", f"{tent_campers:,}")
        col3.metric(f"RV Campers ({latest_year if latest_year else ''})", f"{rv_campers:,}")

        # Description or info
        if park_info.get('description'):
            st.markdown(f"**Description:** {park_info['description']}")


# --- Usage summary function (properly defined) ---
def get_park_usage_summarized(park_name=None, aggregate=False):
    if aggregate:
        resp = requests.get(f"{API_URL}/park_usage_summarized", params={"aggregate": "true"})
    elif park_name is None or park_name == "All Parks":
        resp = requests.get(f"{API_URL}/park_usage_summarized")
    else:
        resp = requests.get(f"{API_URL}/park_usage_summarized", params={"park_name": park_name})
    if resp.status_code == 200:
        return resp.json()
    return []

if selected_park == "All Parks":
    # Show monthly recreation chart for selected parks (Plotly)
    show_monthly_recreation_chart(selected_parks)
    # Show annual totals for all parks
    agg_usage_data = get_park_usage_summarized(aggregate=True)
    if agg_usage_data:
        df_agg = pd.DataFrame(agg_usage_data)
        # Use correct column name for year
        year_col = "Year" if "Year" in df_agg.columns else "year"
        # Remove year 0
        df_agg = df_agg[df_agg[year_col] != 0]
        usage_melt = df_agg.melt(
            id_vars=[year_col],
            value_vars=["total_recreation_visits", "total_non_recreation_visits", "total_concessioner_camping", "total_tent_campers", "total_rv_campers"],
            var_name="VisitType",
            value_name="Visits"
        )
        st.subheader("Annual Totals for All Parks")
        chart_agg = alt.Chart(usage_melt).mark_bar().encode(
            x=alt.X(f"{year_col}:N", title="Year", sort="-y"),
            y=alt.Y("Visits:Q", title="Visits", stack="zero"),
            color=alt.Color("VisitType:N", title="Metric"),
            tooltip=[year_col, "VisitType", "Visits"]
        ).properties(
            width=900,
            height=500,
            title="Annual Totals for All Parks"
        )
        st.altair_chart(chart_agg, use_container_width=True)
    else:
        st.info("No aggregated usage data available.")

    # Also show by park for selected year
    usage_data = get_park_usage_summarized()
    if usage_data:
        df_usage = pd.DataFrame(usage_data)
        # Remove year 0
        df_usage = df_usage[df_usage["Year"] != 0]
        years = sorted(df_usage["Year"].dropna().unique())
        selected_year = st.sidebar.selectbox("Select Year for Usage Chart", years, index=len(years)-1)
        df_usage_year = df_usage[df_usage["Year"] == selected_year]
        # Add all camping and visit metrics to chart
        usage_melt = df_usage_year.melt(
            id_vars=["park_name"],
            value_vars=[
                "total_recreation_visits",
                "total_non_recreation_visits",
                "total_concessioner_camping",
                "total_tent_campers",
                "total_rv_campers"
            ],
            var_name="Metric",
            value_name="Visits"
        )
        chart_usage = alt.Chart(usage_melt).mark_bar().encode(
            x=alt.X("park_name:N", title="Park", sort="-y"),
            y=alt.Y("Visits:Q", title="Visits", stack="zero"),
            color=alt.Color("Metric:N", title="Metric"),
            tooltip=["park_name", "Metric", "Visits"]
        ).properties(
            width=900,
            height=500,
            title=f"Annual Usage & Camping by Park ({selected_year})"
        )
        st.altair_chart(chart_usage, use_container_width=True)
    else:
        pass
else:
    usage_data = get_park_usage_summarized(selected_park)
    if usage_data:
        df_usage = pd.DataFrame(usage_data)
        # Remove year 0
        df_usage = df_usage[df_usage["Year"] != 0]
        # Main usage chart (all metrics)
        usage_melt = df_usage.melt(
            id_vars=["Year"],
            value_vars=[
                "total_recreation_visits",
                "total_non_recreation_visits",
                "total_concessioner_camping",
                "total_tent_campers",
                "total_rv_campers"
            ],
            var_name="Metric",
            value_name="Visits"
        )
        st.subheader(f"Annual Usage & Camping for {selected_park}")
        chart_usage = alt.Chart(usage_melt).mark_bar().encode(
            x=alt.X("Year:N", title="Year", sort="-y"),
            y=alt.Y("Visits:Q", title="Visits", stack="zero"),
            color=alt.Color("Metric:N", title="Metric"),
            tooltip=["Year", "Metric", "Visits"]
        ).properties(
            width=900,
            height=500,
            title=f"Annual Usage & Camping for {selected_park}"
        )
        st.altair_chart(chart_usage, use_container_width=True)

        # Separate camping chart (grouped bars, proportional)
        camping_metrics = [
            "total_concessioner_camping",
            "total_tent_campers",
            "total_rv_campers"
        ]
        camping_melt = df_usage.melt(
            id_vars=["Year"],
            value_vars=camping_metrics,
            var_name="CampingType",
            value_name="Campers"
        )
        st.subheader(f"Annual Camping Breakdown for {selected_park}")
        chart_camping = alt.Chart(camping_melt).mark_bar().encode(
            x=alt.X("Year:N", title="Year", sort="-y"),
            y=alt.Y("Campers:Q", title="Number of Campers"),
            color=alt.Color("CampingType:N", title="Camping Type", scale=alt.Scale(scheme="set2")),
            tooltip=["Year", "CampingType", "Campers"]
        ).properties(
            width=900,
            height=350,
            title=f"Annual Camping Breakdown for {selected_park}"
        )
        st.altair_chart(chart_camping, use_container_width=True)
    else:
        pass


def get_parks_to_landmarks(park_name=None, state_abbr=None, limit=100, offset=0):
    params = {}
    if park_name:
        params["park_name"] = park_name
    if state_abbr:
        params["landmark_state"] = state_abbr
    params["limit"] = limit
    params["offset"] = offset
    resp = requests.get(f"{API_URL}/nps_parks_to_landmarks", params=params)
    if resp.status_code == 200:
        return resp.json()
    return []

st.sidebar.markdown("---")
st.sidebar.markdown("### Parks to Landmarks Table")



# Main page filters for Parks to Nearby Landmarks
st.subheader("Parks to Nearby Landmarks")
col_landmark1, col_landmark2 = st.columns(2)
park_filter = col_landmark1.text_input("Filter by Park Name")
state_filter = col_landmark2.text_input("Filter by State")

# Use filters for API call
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
    resp = requests.get(f"{API_URL}/landmarks_summary", params=params)
    if resp.status_code == 200:
        return resp.json()
    return {}

selected_state_abbr = None
if selected_park != "All Parks":
    park_row = parks_df[parks_df["name"].str.lower() == selected_park.lower()]
    if not park_row.empty:
        # Use first abbreviation if multiple
        selected_state_abbr = park_row.iloc[0]["states"].split(",")[0].strip().upper()

if selected_park == "All Parks":
    summary_data = get_landmarks_summary()
else:
    summary_data = get_landmarks_summary(state_abbr=selected_state_abbr)

if summary_data:
    colorful_palette = [
        "#e6194b", "#3cb44b", "#ffe119", "#4363d8", "#f58231",
        "#911eb4", "#46f0f0", "#f032e6", "#bcf60c", "#fabebe",
        "#008080", "#e6beff", "#9a6324", "#fffac8", "#800000",
        "#aaffc3", "#808000", "#ffd8b1", "#000075", "#808080"
    ]

    # By State
    df_state = pd.DataFrame(summary_data["by_state"])
    st.subheader("Landmark Counts by State (Summary)")
    chart_state = alt.Chart(df_state).mark_bar().encode(
        x=alt.X("state:N", title="State", sort="-y"),
        y=alt.Y("count:Q", title="Count"),
        color=alt.Color("state:N", scale=alt.Scale(range=colorful_palette)),
        tooltip=["state", "state_abbr", "count"]
    ).properties(
        width=700,
        height=400,
        title="Number of Landmarks by State"
    )
    st.altair_chart(chart_state, use_container_width=True)

    # By Category
    df_cat = pd.DataFrame(summary_data["by_category"])
    st.subheader("Landmark Counts by Category of Property")
    chart_cat = alt.Chart(df_cat).mark_bar().encode(
        x=alt.X("category_of_property:N", title="Category", sort="-y"),
        y=alt.Y("count:Q", title="Count"),
        color=alt.Color("category_of_property:N", scale=alt.Scale(range=colorful_palette)),
        tooltip=["category_of_property", "state", "state_abbr", "count"]
    ).properties(
        width=700,
        height=400,
        title="Number of Landmarks by Category of Property"
    )
    st.altair_chart(chart_cat, use_container_width=True)

    # By Level of Significance
    df_level = pd.DataFrame(summary_data["by_level"])
    st.subheader("Landmark Counts by Level of Significance")
    chart_level = alt.Chart(df_level).mark_bar().encode(
        x=alt.X("level_of_significance:N", title="Level of Significance", sort="-y"),
        y=alt.Y("count:Q", title="Count"),
        color=alt.Color("level_of_significance:N", scale=alt.Scale(range=colorful_palette)),
        tooltip=["level_of_significance", "state", "state_abbr", "count"]
    ).properties(
        width=700,
        height=400,
        title="Number of Landmarks by Level of Significance"
    )
    st.altair_chart(chart_level, use_container_width=True)
else:
    st.info("No summary data available for parks-to-landmarks.")