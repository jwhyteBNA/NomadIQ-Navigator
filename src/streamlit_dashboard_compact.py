import requests
import pandas as pd
import numpy as np
import altair as alt
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

API_URL = "http://localhost:8000"


st.markdown("""
	<style>
	/* Aggressively reduce metric label and value font size */
	div[data-testid="metric-container"] * {
		font-size: 0.75rem !important;
	}
	div[data-testid="metric-container"] {
		min-height: 32px;
		margin-bottom: 0.25rem;
	}
	</style>
""", unsafe_allow_html=True)

st.set_page_config(page_title="NomadIQ Navigator Dashboard", layout="wide")


st.markdown("# NomadIQ Navigator Dashboard")


hero = st.container()
with hero:
	# --- Usage Data and Metrics Logic ---
	usage_response = requests.get(f"{API_URL}/parks/usage?granularity=annual&aggregate=false")
	usage = pd.DataFrame(usage_response.json()) if usage_response.status_code == 200 else pd.DataFrame()
	last_year = 2024
	prev_year = 2023
	visitors_last_year = usage[usage['Year'] == last_year]['total_recreation_visits'].sum() if not usage.empty and 'Year' in usage.columns else 0
	visitors_prev = usage[usage['Year'] == prev_year]['total_recreation_visits'].sum() if not usage.empty and 'Year' in usage.columns else 0
	pct_change_yoy = ((visitors_last_year - visitors_prev) / visitors_prev * 100) if visitors_prev else 0
	top5_parks = []
	if not usage.empty and 'Year' in usage.columns and 'park_name' in usage.columns:
		top_rows = usage[(usage['Year'] == last_year) & usage['park_name'].notnull()].sort_values('total_recreation_visits', ascending=False).head(5)
		for _, row in top_rows.iterrows():
			top5_parks.append((row['park_name'], int(row['total_recreation_visits'])))

	col_map, col_right = st.columns([1, 1], gap="large")

	with col_map:
		parks_response = requests.get(f"{API_URL}/parks")
		parks = pd.DataFrame(parks_response.json()) if parks_response.status_code == 200 else pd.DataFrame()
		landmarks_response = requests.get(f"{API_URL}/landmarks")
		landmarks = pd.DataFrame(landmarks_response.json()) if landmarks_response.status_code == 200 else pd.DataFrame()
		map_type = st.selectbox("Show:", ["Parks", "Landmarks"], index=0, key="map_type_select")
		if map_type == "Parks" and not parks.empty:
			if "latitude" in parks.columns and "longitude" in parks.columns:
				fig = px.scatter_geo(
					parks,
					lat="latitude",
					lon="longitude",
					hover_name="name",
					color="designation",
					scope="north america",
					title="National Parks Locations",
					labels={"designation": "Designation"}
				)
				fig.update_geos(
					center=dict(lat=39.8283, lon=-98.5795),
					projection_scale=2.2
				)
				st.plotly_chart(fig, use_container_width=True)
			else:
				st.warning("Park latitude/longitude data not available.")
		elif map_type == "Landmarks" and not landmarks.empty:
			city_state_coords = {
				("New York", "NY"): (40.7128, -74.0060),
				("Los Angeles", "CA"): (34.0522, -118.2437),
				("Chicago", "IL"): (41.8781, -87.6298),
				("Houston", "TX"): (29.7604, -95.3698),
				("Phoenix", "AZ"): (33.4484, -112.0740),
			}
			state_coords = {
				"AL": (32.806671, -86.791130), "AK": (61.370716, -152.404419), "AZ": (33.729759, -111.431221),
				"AR": (34.969704, -92.373123), "CA": (36.116203, -119.681564), "CO": (39.059811, -105.311104),
				"CT": (41.597782, -72.755371), "DE": (39.318523, -75.507141), "FL": (27.766279, -81.686783),
				"GA": (33.040619, -83.643074), "HI": (21.094318, -157.498337), "ID": (44.240459, -114.478828),
				"IL": (40.349457, -88.986137), "IN": (39.849426, -86.258278), "IA": (42.011539, -93.210526),
				"KS": (38.526600, -96.726486), "KY": (37.668140, -84.670067), "LA": (31.169546, -91.867805),
				"ME": (44.693947, -69.381927), "MD": (39.063946, -76.802101), "MA": (42.230171, -71.530106),
				"MI": (43.326618, -84.536095), "MN": (45.694454, -93.900192), "MS": (32.741646, -89.678696),
				"MO": (38.456085, -92.288368), "MT": (46.921925, -110.454353), "NE": (41.125370, -98.268082),
				"NV": (38.313515, -117.055374), "NH": (43.452492, -71.563896), "NJ": (40.298904, -74.521011),
				"NM": (34.840515, -106.248482), "NY": (42.165726, -74.948051), "NC": (35.630066, -79.806419),
				"ND": (47.528912, -99.784012), "OH": (40.388783, -82.764915), "OK": (35.565342, -96.928917),
				"OR": (44.572021, -122.070938), "PA": (40.590752, -77.209755), "RI": (41.680893, -71.511780),
				"SC": (33.856892, -80.945007), "SD": (44.299782, -99.438828), "TN": (35.747845, -86.692345),
				"TX": (31.054487, -97.563461), "UT": (40.150032, -111.862434), "VT": (44.045876, -72.710686),
				"VA": (37.769337, -78.169968), "WA": (47.400902, -121.490494), "WV": (38.491226, -80.954578),
				"WY": (42.755966, -107.302490), "WI": (44.268543, -89.616508)
			}
			def get_coords(row):
				city, state_abbr = row.get("city"), row.get("state_abbr")
				coords = city_state_coords.get((city, state_abbr))
				if coords:
					return coords
				lat, lon = state_coords.get(state_abbr, (None, None))
				if lat is not None and lon is not None:
					lat += np.random.uniform(-0.2, 0.2)
					lon += np.random.uniform(-0.2, 0.2)
				return (lat, lon)
			landmarks["latitude"], landmarks["longitude"] = zip(*landmarks.apply(get_coords, axis=1))
			fig = px.scatter_geo(
				landmarks.dropna(subset=["latitude", "longitude"]),
				lat="latitude",
				lon="longitude",
				hover_name="property_name" if "property_name" in landmarks.columns else "state",
				color="category_of_property" if "category_of_property" in landmarks.columns else None,
				scope="north america",
				title="Landmarks Locations"
			)
			fig.update_geos(
				center=dict(lat=39.8283, lon=-98.5795),
				projection_scale=2.2
			)
			st.plotly_chart(fig, use_container_width=True)

	# --- Right Column: Metrics, Top 5 Parks, Annual Totals ---
	with col_right:
		alerts_response = requests.get(f"{API_URL}/parks/alerts")
		alerts = pd.DataFrame(alerts_response.json()) if alerts_response.status_code == 200 else pd.DataFrame()
		parks_with_closure_alerts = 0
		pct_parks_with_alerts = 0
		if not alerts.empty and 'park_name' in alerts.columns and 'alert_category' in alerts.columns:
			date_col = 'lastIndexedDate' if 'lastIndexedDate' in alerts.columns else None
			if date_col:
				alerts_month = alerts.copy()
				alerts_month[date_col] = pd.to_datetime(alerts_month[date_col], errors='coerce')
				alerts_month = alerts_month[alerts_month[date_col].dt.month == 9]
				alerts_month = alerts_month[alerts_month[date_col].dt.year == 2025]
				closure_alerts_month = alerts_month[alerts_month['alert_category'].str.contains('closure', case=False, na=False)]
				parks_with_closure_alerts = closure_alerts_month['park_name'].nunique()
				parks_with_any_alert = alerts_month['park_name'].nunique()
				total_parks = parks['name'].nunique() if not parks.empty and 'name' in parks.columns else 0
				pct_parks_with_alerts = (parks_with_any_alert / total_parks * 100) if total_parks else 0
			else:
				parks_with_closure_alerts = 0
				pct_parks_with_alerts = 0
		st.markdown("""
		<div style='background-color:#222; border-radius:8px; padding:1rem 0.5rem; margin-bottom:1rem;'>
			<div style='display: flex; flex-direction: row; justify-content: center; gap: 2rem;'>
				<div style='text-align:center;'>
					<span style='font-size:0.9rem; font-weight:600; color:#fff;'>Parks with Any Closure Alerts</span><br>
					<span style='font-size:1.1rem; font-weight:bold; color:#fff;'>{parks_with_closure_alerts}</span>
				</div>
				<div style='text-align:center;'>
					<span style='font-size:0.9rem; font-weight:600; color:#fff;'>% of Parks with Any Alerts (This Month)</span><br>
					<span style='font-size:1.1rem; font-weight:bold; color:#fff;'>{pct_parks_with_alerts:.1f}%</span>
				</div>
			</div>
		</div>
		""".format(parks_with_closure_alerts=parks_with_closure_alerts, pct_parks_with_alerts=pct_parks_with_alerts), unsafe_allow_html=True)

		# Top 5 Parks and Annual Totals side by side
		col_top5, col_annual = st.columns([1, 1], gap="small")
		with col_top5:
			if top5_parks:
				st.markdown("""
				<h4 style='font-size:1.1rem; margin-bottom:0.5rem;'>Top 5 Parks by Visitors (2024)</h4>
				<div style='display: flex; flex-direction: column; align-items: center; gap: 0.25rem; padding: 0; margin: 0;'>
				""", unsafe_allow_html=True)
				colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
				for i, (park_name, visitors) in enumerate(top5_parks):
					color = colors[i % len(colors)]
					st.markdown(f"<div style='margin:0; padding:0;'><span style='color:{color}; font-weight:bold;'>{park_name}</span><br><span style='font-size:1.1rem; font-weight:bold;'>{visitors:,}</span></div>", unsafe_allow_html=True)
				st.markdown("</div>", unsafe_allow_html=True)
		with col_annual:
			st.markdown("""
			<h4 style='font-size:1.1rem; margin-bottom:0.5rem;'>2024 Annual Totals</h4>
			<div style='display: flex; flex-direction: column; align-items: center; gap: 0.25rem; padding: 0; margin: 0;'>
			""", unsafe_allow_html=True)
			annual_totals_response = requests.get(f"{API_URL}/parks/usage?year=2024&granularity=annual&aggregate=true")
			annual_totals_raw = annual_totals_response.json() if annual_totals_response.status_code == 200 else {}
			if isinstance(annual_totals_raw, list) and len(annual_totals_raw) > 0:
				annual_totals = annual_totals_raw[0]
			else:
				annual_totals = annual_totals_raw if isinstance(annual_totals_raw, dict) else {}
			metrics = [
				("Total Recreation Visits", annual_totals.get('total_recreation_visits', 0)),
				("Total Non-Recreation Visits", annual_totals.get('total_non_recreation_visits', 0)),
				("Concessioner Camping", annual_totals.get('total_concessioner_camping', 0)),
				("Tent Campers", annual_totals.get('total_tent_campers', 0)),
				("RV Campers", annual_totals.get('total_rv_campers', 0)),
			]
			colors = ["#fff", "#999"]
			for i, (label, value) in enumerate(metrics):
				color = colors[i % len(colors)]
				st.markdown(f"<div style='margin:0; padding:0;'><span style='font-weight:bold; color:{color};'>{label}</span><br><span style='font-size:1.1rem; font-weight:bold; color:{color};'>{value:,}</span></div>", unsafe_allow_html=True)
			st.markdown(f"<div style='margin:0; padding:0;'><span style='font-weight:bold; color:#999;'>% Change YoY (2024 vs 2023)</span><br><span style='font-size:1.1rem; font-weight:bold; color:#999;'>{pct_change_yoy:.1f}%</span></div>", unsafe_allow_html=True)
			st.markdown("</div>", unsafe_allow_html=True)


# --- Second Row (Current Conditions) ---
current = st.container()
with current:
	col_alerts, col_heatmap = st.columns([1, 1])
	with col_alerts:
		st.subheader("Alert Summary & Table")
		alerts_response = requests.get(f"{API_URL}/parks/alerts")
		alerts = pd.DataFrame(alerts_response.json()) if alerts_response.status_code == 200 else pd.DataFrame()
		date_col = 'lastIndexedDate' if 'lastIndexedDate' in alerts.columns else None
		date_filter = st.selectbox("Show alerts for:", ["This Month", "This Year", "Since Origin"], index=0, key="alert_date_filter")

		filtered_alerts = alerts.copy()
		donut_title = "Alert Summary"
		if date_col:
			filtered_alerts[date_col] = pd.to_datetime(filtered_alerts[date_col], errors='coerce')
			if date_filter == "This Month":
				now = pd.Timestamp.now()
				filtered_alerts = filtered_alerts[(filtered_alerts[date_col].dt.month == now.month) & (filtered_alerts[date_col].dt.year == now.year)]
				donut_title = f"Alert Summary ({now.strftime('%B %Y')})"
			elif date_filter == "This Year":
				now = pd.Timestamp.now()
				filtered_alerts = filtered_alerts[filtered_alerts[date_col].dt.year == now.year]
				donut_title = f"Alert Summary ({now.year})"
			else:
				donut_title = "Alert Summary (All Time)"
		if not filtered_alerts.empty and 'alert_category' in filtered_alerts.columns:
			valid_alerts = filtered_alerts[filtered_alerts['alert_category'].apply(lambda x: isinstance(x, str) and x.strip() != '')]
			alert_counts = valid_alerts['alert_category'].value_counts().reset_index()
			alert_counts.columns = ['Category', 'Count']
			if not alert_counts.empty:
				fig_donut = px.pie(alert_counts, names='Category', values='Count', hole=0.5, title=donut_title)
				st.plotly_chart(fig_donut, use_container_width=True, key="donut_chart")
			else:
				st.info(f"No valid alert categories to display for selected range.")
		else:
			st.info(f"No alert data available for selected range.")

	with col_heatmap:
		heatmap_title = "Active Alerts by Park"
		if date_col:
			if date_filter == "This Month":
				now = pd.Timestamp.now()
				heatmap_title += f" ({now.strftime('%B %Y')})"
			elif date_filter == "This Year":
				now = pd.Timestamp.now()
				heatmap_title += f" ({now.year})"
			else:
				heatmap_title += " (All Time)"
		if not filtered_alerts.empty and 'park_name' in filtered_alerts.columns:
			park_counts = filtered_alerts['park_name'].value_counts().reset_index()
			park_counts.columns = ['Park', 'Alert Count']
			fig_bar = px.bar(
				park_counts,
				x='Alert Count',
				y='Park',
				orientation='h',
				color='Alert Count',
				color_continuous_scale='Reds',
				labels={'Alert Count': 'Alert Count', 'Park': 'Park Name'},
				title=heatmap_title
			)
			fig_bar.update_layout(margin=dict(l=0, r=0, t=40, b=0), yaxis={'categoryorder':'total ascending'})
			st.plotly_chart(fig_bar, use_container_width=True)
		else:
			st.info(f"No park alert data available for selected range.")

# --- Third Row (Trends) ---
trends = st.container()
with trends:
	st.markdown("## Park Visitor Trends")
	usage_response = requests.get(f"{API_URL}/parks/usage?granularity=annual&aggregate=false")
	usage = pd.DataFrame(usage_response.json()) if usage_response.status_code == 200 else pd.DataFrame()
	park_options = usage['park_name'].dropna().unique().tolist() if not usage.empty and 'park_name' in usage.columns else []
	selected_park = st.selectbox("Filter by Park:", ["All Parks"] + park_options, index=0)

	x_col = 'Year'
	title_suffix = 'Year'
	if selected_park == "All Parks":
		df_line = usage.groupby(x_col, as_index=False)['total_recreation_visits'].sum()
		title_line = f"Total Visitors per {title_suffix} (All Parks)"
	else:
		df_line = usage[usage['park_name'] == selected_park].groupby(x_col, as_index=False)['total_recreation_visits'].sum()
		title_line = f"Total Visitors per {title_suffix} ({selected_park})"
	if not df_line.empty:
		fig_line = px.line(df_line, x=x_col, y='total_recreation_visits', markers=True, title=title_line, labels={'total_recreation_visits': 'Total Visitors'})
		fig_line.update_traces(line=dict(width=3), marker=dict(size=8))
		fig_line.update_layout(margin=dict(l=0, r=0, t=40, b=0))
		st.plotly_chart(fig_line, use_container_width=True)
	else:
		st.info(f"No visitor data available for selected park and {title_suffix.lower()}.")

	st.markdown(f"### Visitor Type Breakdown (Facet Grid, {title_suffix})")
	visitor_types = ['total_recreation_visits', 'total_non_recreation_visits', 'total_concessioner_camping', 'total_tent_campers', 'total_rv_campers']
	visitor_type_labels = {
    "total_recreation_visits": "Total Recreation Visits",
    "total_non_recreation_visits": "Total Non-Recreation Visits",
    "total_concessioner_camping": "Total Concessioner Camping",
    "total_tent_campers": "Total Tent Campers",
    "total_rv_campers": "Total RV Campers"
}

	if selected_park == "All Parks":
		df_facet = usage.groupby(x_col, as_index=False)[visitor_types].sum()
	else:
		df_facet = usage[usage['park_name'] == selected_park].groupby(x_col, as_index=False)[visitor_types].sum()
	if not df_facet.empty:
		df_facet_melt = df_facet.melt(id_vars=x_col, value_vars=visitor_types, var_name='Visitor Type', value_name='Count')
		df_facet_melt['Visitor Type'] = df_facet_melt['Visitor Type'].map(visitor_type_labels)
		fig_facet = px.line(
			df_facet_melt,
			x=x_col,
			y='Count',
			facet_col='Visitor Type',
			color='Visitor Type',
			facet_col_wrap=2,
			title="Park Visitors by Type",
			markers=True
		)
		fig_facet.update_yaxes(matches=None)
		fig_facet.update_layout(margin=dict(l=0, r=0, t=40, b=0), showlegend=False)
		st.plotly_chart(fig_facet, use_container_width=True)

	# --- Fourth Row (Landmarks) ---
	landmarks_row = st.container()
	with landmarks_row:
		st.markdown("### Landmarks Overview")
		col_lm_bar, col_lm_treemap = st.columns([1, 1])
		landmarks_response = requests.get(f"{API_URL}/landmarks")
		landmarks = pd.DataFrame(landmarks_response.json()) if landmarks_response.status_code == 200 else pd.DataFrame()
        
		with col_lm_bar:
			if not landmarks.empty and 'level_of_significance' in landmarks.columns:
				filtered = landmarks[landmarks['level_of_significance'] != 'International']
				level_counts = filtered['level_of_significance'].value_counts().reset_index()
				level_counts.columns = ['level_of_significance', 'count']
				fig_bar = px.bar(
					level_counts,
					x='level_of_significance',
					y='count',
					color='level_of_significance',
					title="Number of Landmarks by Level of Significance (Excluding International)",
					labels={
						'level_of_significance': 'Level of Significance',
						'count': 'Count'
					}
				)
				st.plotly_chart(fig_bar, use_container_width=True)
		with col_lm_treemap:
			if not landmarks.empty and 'state_abbr' in landmarks.columns and 'category_of_property' in landmarks.columns:
				lm_valid = landmarks.dropna(subset=['state_abbr', 'category_of_property'])
				if not lm_valid.empty:
					fig_treemap = px.treemap(
						lm_valid,
						path=['state_abbr', 'category_of_property'],
						values=None,
						color='category_of_property',
						title="Landmarks by State and Category of Property"
					)
					st.plotly_chart(fig_treemap, use_container_width=True)