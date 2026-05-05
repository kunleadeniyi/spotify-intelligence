import streamlit as st

from dashboard.views import live, wrapped, recommendations

st.set_page_config(layout="wide", page_title="Spotify Intelligence", page_icon="🎵")

st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;700&display=swap" rel="stylesheet">
    <style>
    /* Apply Space Grotesk to content elements only.
       Deliberately excludes span, button, div — Streamlit renders
       Material Symbols icons as text inside those elements and they
       must keep their icon font to display correctly. */
    html, body, p, h1, h2, h3, h4, h5, h6,
    input, textarea, label, td, th, li, a,
    [data-testid="stMarkdownContainer"],
    [data-testid="stText"],
    [data-testid="stCaption"],
    [data-testid="stMetricLabel"],
    [data-testid="stMetricValue"] {
        font-family: 'Space Grotesk', sans-serif !important;
    }
    </style>
""", unsafe_allow_html=True)

VIEWS = {
    "Live": live.render,
    "Wrapped": wrapped.render,
    "Recommendations": recommendations.render
}

with st.sidebar:
    st.title("Spotify Intelligence")
    selected = st.radio("Navigate", list(VIEWS.keys()), label_visibility="collapsed")


VIEWS[selected]()