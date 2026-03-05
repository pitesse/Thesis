"""
f1 strategy dashboard — live race monitor powered by streamlit + kafka.

## how to run
1. make sure the docker stack is up (kafka on localhost:9092):
       ./run_simulation.sh --speed 50
2. in a separate terminal, install dependencies and launch the dashboard:
       pip install -r dashboard/requirements.txt
       streamlit run dashboard/app.py
"""

import json
import time
from datetime import timedelta

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# page config
st.set_page_config(page_title="F1 Pit Wall", layout="wide")
st.title("F1 Pit Wall — Live Race Monitor")

KAFKA_BROKER = "localhost:9092"
TOPIC_LAPS = "f1-laps"
TOPIC_TRACK_STATUS = "f1-track-status"
TOPIC_ALERTS = "f1-alerts"

# track status code -> human readable label and color
TRACK_STATUS_MAP = {
    "1": ("Green", "#00ff00"),
    "2": ("Yellow", "#ffff00"),
    "3": ("SC (Safety Car)", "#ff8c00"),
    "4": ("Red", "#ff0000"),
    "5": ("VSC (Virtual Safety Car)", "#ff8c00"),
    "6": ("VSC Ending", "#bfff00"),
    "7": ("SC Ending", "#bfff00"),
}


def format_lap_time(seconds):
    """convert lap time in seconds to mm:ss.fff display string.
    ex: 82.437 -> '1:22.437'
    """
    if seconds is None or pd.isna(seconds):
        return "—"
    td = timedelta(seconds=float(seconds))
    total_seconds = td.total_seconds()
    minutes = int(total_seconds // 60)
    secs = total_seconds - minutes * 60
    return f"{minutes}:{secs:06.3f}"


def create_consumer():
    """create a kafka consumer subscribed to lap, track status, and flink alert topics.
    uses a short poll timeout (200ms) so the streamlit loop stays responsive.
    consumer_timeout_ms prevents blocking indefinitely when no messages arrive.
    """
    return KafkaConsumer(
        TOPIC_LAPS,
        TOPIC_TRACK_STATUS,
        TOPIC_ALERTS,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="f1-dashboard",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=200,
    )


def try_connect():
    """attempt kafka connection with retries. returns consumer or None."""
    try:
        return create_consumer()
    except NoBrokersAvailable:
        return None


# session state: persist leaderboard and track status across streamlit reruns
if "leaderboard" not in st.session_state:
    st.session_state.leaderboard = {}
if "track_status" not in st.session_state:
    st.session_state.track_status = ("Green", "#00ff00")
if "track_message" not in st.session_state:
    st.session_state.track_message = "Waiting for data..."
if "alerts" not in st.session_state:
    st.session_state.alerts = []

# ui placeholders: using st.empty() so we can overwrite them each loop iteration
status_placeholder = st.empty()
st.divider()
st.subheader("Leaderboard")
leaderboard_placeholder = st.empty()
st.divider()
st.subheader("Live Strategic Alerts")
alerts_placeholder = st.empty()

# connection status
conn_placeholder = st.sidebar.empty()

consumer = try_connect()
if consumer is None:
    st.error(
        "Could not connect to Kafka at `localhost:9092`. "
        "Make sure the Docker stack is running (`./run_simulation.sh`)."
    )
    st.stop()

conn_placeholder.success("Connected to Kafka")

# main polling loop: continuously fetch new messages and update the ui.
# streamlit reruns the entire script on each interaction, but the while True
# loop keeps it alive for streaming updates between interactions.
while True:
    try:
        messages = consumer.poll(timeout_ms=500, max_records=100) # type: ignore
    except Exception:
        # if consumer disconnects, try to reconnect
        time.sleep(2)
        consumer = try_connect()
        if consumer is None:
            conn_placeholder.error("Kafka disconnected. Retrying...")
            time.sleep(3)
            continue
        conn_placeholder.success("Reconnected to Kafka")
        continue

    for topic_partition, records in messages.items():
        for record in records:
            msg = record.value
            topic = record.topic

            if topic == TOPIC_TRACK_STATUS:
                code = str(msg.get("Status", "1"))
                label, color = TRACK_STATUS_MAP.get(
                    code, (msg.get("Message", "Unknown"), "#888888")
                )
                st.session_state.track_status = (label, color)
                st.session_state.track_message = msg.get("Message", label)

            elif topic == TOPIC_LAPS:
                driver = msg.get("Driver")
                if driver:
                    st.session_state.leaderboard[driver] = {
                        "Position": msg.get("Position"),
                        "Driver": driver,
                        "Lap": msg.get("LapNumber"),
                        "Lap Time": format_lap_time(msg.get("LapTime")),
                        "Compound": msg.get("Compound", "—"),
                        "Tyre Life": msg.get("TyreLife", "—"),
                        "Gap Ahead": (
                            f"+{msg['GapToCarAhead']:.3f}s"
                            if msg.get("GapToCarAhead") is not None
                            else "Leader"
                        ),
                    }

            elif topic == TOPIC_ALERTS:
                # classify alert type from json fields emitted by flink's JsonSerializer.
                # each alert pojo serializes to different keys, ex: tireDropAlerts have "delta",
                # liftCoastAlerts have "brakeDate", pitWindowAlerts have "gapBehind".
                if "brakeDate" in msg:
                    alert_type = "Lift & Coast"
                    summary = f"{msg.get('driver', '?')} — lift→brake"
                elif "delta" in msg:
                    alert_type = "Tire Drop"
                    summary = (
                        f"{msg.get('driver', '?')} Lap {msg.get('lapNumber', '?')} "
                        f"— delta +{msg.get('delta', 0):.3f}s"
                    )
                elif "gapBehind" in msg:
                    alert_type = "Pit Window"
                    summary = (
                        f"{msg.get('driver', '?')} — gap {msg.get('gapBehind', 0):.1f}s "
                        f"(threshold {msg.get('threshold', 0):.1f}s)"
                    )
                else:
                    alert_type = "Alert"
                    summary = json.dumps(msg, default=str)[:120]

                st.session_state.alerts.append(
                    {
                        "Type": alert_type,
                        "Detail": summary,
                    }
                )
                # keep only the most recent 50 alerts to bound memory
                if len(st.session_state.alerts) > 50:
                    st.session_state.alerts = st.session_state.alerts[-50:]

    # render track status banner
    label, color = st.session_state.track_status
    status_placeholder.markdown(
        f"""
        <div style="
            background-color: {color}20;
            border-left: 6px solid {color};
            padding: 12px 20px;
            border-radius: 4px;
            margin-bottom: 8px;
        ">
            <span style="font-size: 14px; color: #aaa;">TRACK STATUS</span><br>
            <span style="font-size: 32px; font-weight: bold; color: {color};">
                {label}
            </span>
            <span style="font-size: 14px; color: #999; margin-left: 16px;">
                {st.session_state.track_message}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # render leaderboard
    if st.session_state.leaderboard:
        df = pd.DataFrame(st.session_state.leaderboard.values())
        df = df.sort_values("Position").reset_index(drop=True)
        df.index = df.index + 1  # 1-based display index
        leaderboard_placeholder.dataframe(df, width="stretch", hide_index=True)
    else:
        leaderboard_placeholder.info("Waiting for lap data...")

    # render live alerts (most recent 10, newest first)
    if st.session_state.alerts:
        recent = st.session_state.alerts[-10:][::-1]
        alerts_df = pd.DataFrame(recent)
        alerts_placeholder.dataframe(alerts_df, width="stretch", hide_index=True)
    else:
        alerts_placeholder.info("Waiting for Flink alerts...")

    time.sleep(0.5)
