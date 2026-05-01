import streamlit as st

from dashboard.views import live, wrapped, recommendations

st.set_page_config(layout="wide", page_title="Spotify Intelligence", page_icon="🎵")

st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;700&display=swap" rel="stylesheet">
    <style>
    *, *::before, *::after { font-family: 'Space Grotesk', sans-serif !important; }
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