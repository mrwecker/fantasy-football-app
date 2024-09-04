import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import os

# Load the data using st.cache_data to improve performance
@st.cache_data
def load_data():
    base_path = os.path.dirname(__file__)  # Directory of the current script
    file_path = os.path.join(base_path, "data_processed_player_app.csv")

    # Debug: Check if the file path is correct and if the file exists
    if not os.path.exists(file_path):
        st.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if file not found

    return pd.read_csv(file_path)

# Load the processed data
df = load_data()

# If the data is empty, stop further execution
if df.empty:
    st.stop()

# Display the introductory text
st.markdown("""
    # Welcome to the Fantasy Football Player Selector
    
    This tool will enable you to make calculated decisions on which player to purchase based on a metric calculated from historical statistics. 

    Players' current form is weighted against an average difficulty of the upcoming 5 matches to predict how many points they could make which determines the 'Fixture Difficulty Index'. The higher the number, the better the higher likelihood that a player is in good form, playing against easier teams, and is expected to make more points. This can be viewed easily by the size of the circle. The bigger the better!

    Firstly, use the filters on the left of the page to filter by: 
    1. Position
    2. Cost
    3. Team

    You can have multiple positions and teams, but to compare players, it is recommended to view them by individual positions due to how the players' form is calculated. 
    Players with high form and easy fixtures will be located closer to the top left. Players with poor form and difficult fixtures will be located closer to the bottom right. Once you have located the best few players (near the top left and larger circles), you can hover over them to see their name, team, and price. You can see further statistics in the table below. 

    Good luck!
""")

# Sidebar filters
st.sidebar.header("Filters")

# Position filter
positions = st.sidebar.multiselect(
    'Select Position(s)',
    options=df['Position'].unique(),
    default=df['Position'].unique()
)

# Cost range filter
min_cost = df['Cost'].min()
max_cost = df['Cost'].max()
cost_range = st.sidebar.slider(
    'Select Player Cost Range',
    min_value=int(min_cost),
    max_value=int(max_cost),
    value=(int(min_cost), int(max_cost))
)

# Team filter
teams = st.sidebar.multiselect(
    'Select Team(s)',
    options=df['Team'].unique(),
    default=df['Team'].unique()
)

# Apply filters
filtered_df = df[
    (df['Position'].isin(positions)) &
    (df['Cost'].between(*cost_range)) &
    (df['Team'].isin(teams))
]

# Generate the scatter plot with px
fig_px = px.scatter(
    filtered_df,
    x='Difficulty_score',
    y='Player form',
    color='Fixture Difficulty Index',
    size='Fixture Difficulty Index',
    labels={
        'Player form': 'Current Form/Expected Points',
        'Difficulty_score': 'Difficulty Average for Upcoming 5 Games'
    },
    hover_data={'full_name': True, 'Fixture Difficulty Index': True, 'Team': True, 'Cost': True},
    title='Current Form vs Difficulty of Upcoming Games for Midfielders',
    size_max=30,
    color_continuous_scale=[
        (0.0, 'red'),
        (0.2, 'orange'),
        (0.4, 'yellow'),
        (0.9, 'green'),
        (1.0, 'darkgreen')
    ]
)

# Define gradient background
x = np.linspace(0, 20, 100)  # Adjust x-axis range as needed
y = np.linspace(-2, 15, 100) # Adjust y-axis range as needed
X, Y = np.meshgrid(x, y)

# Calculate the gradient (increase diagonal effect)
Z = np.sqrt(((X - 20) / 30) ** 2 + ((Y - 13) / 7.5) ** 2)  # Shift the diagonal
Z = (Z - np.min(Z)) / (np.max(Z) - np.min(Z))  # Normalize to [0, 1]

# Create heatmap with diagonal gradient
fig_bg = go.Figure()
fig_bg.add_trace(
    go.Heatmap(
        z=Z,
        x=x,
        y=y,
        colorscale=[[0, 'green'], [1, 'red']],  # Green to red transition
        showscale=False,  # Hide the color scale bar
        zmin=0,
        zmax=1,
        opacity=0.6  # Adjust opacity if needed
    )
)

# Overlay the scatter plot on top of the gradient background
for trace in fig_px.data:
    fig_bg.add_trace(trace)

# Add color scale title using annotations
fig_bg.add_annotation(
    x=1.35,  # Position further to the right of the plot
    y=1.05,  # Position at the top of the plot
    text="Fixture Difficulty Index",
    showarrow=False,
    xref="paper",
    yref="paper",
    font=dict(size=14)
)

# Update layout to ensure heatmap covers entire chart
fig_bg.update_layout(
    xaxis=dict(
        title='Difficulty Average for Upcoming 5 Games',  # X-axis title
        range=[0, 21]  # Match x-axis range to heatmap
    ),
    yaxis=dict(
        title='Current Form/Expected Points',  # Y-axis title
        range=[-2, 15]  # Match y-axis range to heatmap
    ),
    showlegend=False,
    height=800,
    margin=dict(r=200)  # Increase the right margin to accommodate the annotation
)

# Set axis ranges to focus on relevant data
fig_bg.update_xaxes(range=[9, 20.1])
fig_bg.update_yaxes(range=[0, 15])

# Display the plot
st.plotly_chart(fig_bg)

# Display the filtered data
st.dataframe(filtered_df)
