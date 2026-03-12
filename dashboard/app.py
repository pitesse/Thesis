"""
f1 strategy dashboard — live race monitor powered by streamlit + kafka.

## how to run
1. make sure the docker stack is up:
       ./run_simulation.sh --speed 50
2. the dashboard starts automatically as a docker service at http://localhost:8501
"""

import json
import os
import time
from datetime import timedelta

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# page config
st.set_page_config(page_title="F1 Pit Wall", layout="wide")
st.title("F1 Pit Wall — Live Race Monitor")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
TOPIC_LAPS = "f1-laps"
TOPIC_TRACK_STATUS = "f1-track-status"
TOPIC_ALERTS = "f1-alerts"

# max laps to keep in the gap evolution chart history
GAP_HISTORY_MAX_LAPS = 40

# max alerts to keep per category in session state
MAX_ALERTS_PER_CATEGORY = 20

# track status code -> human readable label and color (fia standard codes)
TRACK_STATUS_MAP = {
    "1": ("Green", "#00ff00"),
    "2": ("Yellow", "#ffff00"),
    "4": ("SC (Safety Car)", "#ff8c00"),
    "5": ("Red Flag", "#ff0000"),
    "6": ("VSC (Virtual Safety Car)", "#ff8c00"),
    "7": ("VSC Ending", "#bfff00"),
}

# color map for pit suggestion labels
SUGGESTION_LABEL_COLORS = {
    "MONITOR": "#f0ad4e",  # amber/yellow
    "GOOD_PIT": "#5cb85c",  # green
    "PIT_NOW": "#d9534f",  # red
    "LOST_CHANCE": "#888888",  # gray
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
    """create a kafka consumer subscribed to lap, track status, and flink alert topics."""
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


def style_suggestion_label(val):
    """apply background color to suggestion label cells."""
    color = SUGGESTION_LABEL_COLORS.get(val, "transparent")
    return f"background-color: {color}; color: white; font-weight: bold; border-radius: 4px; padding: 2px 6px;"


def bound_alerts(alert_list):
    """keep only the most recent MAX_ALERTS_PER_CATEGORY entries."""
    if len(alert_list) > MAX_ALERTS_PER_CATEGORY:
        return alert_list[-MAX_ALERTS_PER_CATEGORY:]
    return alert_list


# session state: persist data across streamlit reruns
if "leaderboard" not in st.session_state:
    st.session_state.leaderboard = {}
if "track_status" not in st.session_state:
    st.session_state.track_status = ("Green", "#00ff00")
if "track_message" not in st.session_state:
    st.session_state.track_message = "Waiting for data..."

# per-category alert lists (bounded to MAX_ALERTS_PER_CATEGORY each)
if "pit_strategy_alerts" not in st.session_state:
    st.session_state.pit_strategy_alerts = []
if "tire_drop_alerts" not in st.session_state:
    st.session_state.tire_drop_alerts = []
if "lift_coast_alerts" not in st.session_state:
    st.session_state.lift_coast_alerts = []
if "pit_eval_alerts" not in st.session_state:
    st.session_state.pit_eval_alerts = []
if "drop_zone_alerts" not in st.session_state:
    st.session_state.drop_zone_alerts = []

# gap history: {driver: {lap_number: cumulative_gap_seconds}}
if "gap_history" not in st.session_state:
    st.session_state.gap_history = {}
if "lap_race_times" not in st.session_state:
    st.session_state.lap_race_times = {}

# track status banner (above the columns)
status_placeholder = st.empty()
st.divider()

# main layout: two columns
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("Leaderboard")
    leaderboard_placeholder = st.empty()

    st.divider()
    st.subheader("Pit Strategy")
    pit_strategy_placeholder = st.empty()

    st.divider()
    st.subheader("Pit Stop Evaluations")
    pit_eval_placeholder = st.empty()

with col_right:
    st.subheader("Gap to Leader Evolution")
    gap_chart_placeholder = st.empty()

    st.divider()
    st.subheader("Tire Age Tracker")
    tire_chart_placeholder = st.empty()

    st.divider()
    st.subheader("Tire Degradation")
    tire_drop_placeholder = st.empty()

    st.divider()
    st.subheader("Lift & Coast")
    lift_coast_placeholder = st.empty()

# connection status
conn_placeholder = st.sidebar.empty()

consumer = try_connect()
if consumer is None:
    st.error(
        f"Could not connect to Kafka at `{KAFKA_BROKER}`. "
        "Make sure the Docker stack is running (`./run_simulation.sh`)."
    )
    st.stop()

conn_placeholder.success("Connected to Kafka")

# main polling loop
while True:
    try:
        messages = consumer.poll(timeout_ms=500, max_records=100)
    except Exception:
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
                    lap_num = msg.get("LapNumber")
                    tyre_life = msg.get("TyreLife", 0)

                    st.session_state.leaderboard[driver] = {
                        "Position": msg.get("Position"),
                        "Driver": driver,
                        "Lap": lap_num,
                        "Lap Time": format_lap_time(msg.get("LapTime")),
                        "Compound": msg.get("Compound", "---"),
                        "Tyre Life": tyre_life if tyre_life else "---",
                        "Gap Ahead": (
                            f"+{msg['GapToCarAhead']:.3f}s"
                            if msg.get("GapToCarAhead") is not None
                            else "Leader"
                        ),
                    }

                    # track cumulative gap to leader for the evolution chart
                    if lap_num is not None:
                        if lap_num not in st.session_state.lap_race_times:
                            st.session_state.lap_race_times[lap_num] = {}
                        st.session_state.lap_race_times[lap_num][driver] = {
                            "position": msg.get("Position") or 99,
                            "gap_to_car_ahead": msg.get("GapToCarAhead"),
                        }

                        lap_data = st.session_state.lap_race_times[lap_num]
                        sorted_drivers = sorted(
                            lap_data.items(), key=lambda x: x[1]["position"] or 99
                        )
                        cumulative = 0.0
                        for drv, info in sorted_drivers:
                            gap_ahead = info["gap_to_car_ahead"]
                            if gap_ahead is not None and info["position"] > 1:
                                cumulative += float(gap_ahead)
                            else:
                                cumulative = 0.0
                            if drv not in st.session_state.gap_history:
                                st.session_state.gap_history[drv] = {}
                            st.session_state.gap_history[drv][lap_num] = round(
                                cumulative, 3
                            )

            elif topic == TOPIC_ALERTS:
                # classify alert type from json fields emitted by flink's JsonSerializer.
                # each alert pojo serializes to different keys:
                #   pit suggestions: "suggestionLabel" (Phase 2 multi-label field)
                #   pit evaluations: "result" (Phase 1 8-label enum)
                #   tire drops: "delta"
                #   lift & coast: "brakeDate"
                #   drop zones: "positionsLost"
                if "suggestionLabel" in msg:
                    label = msg.get("suggestionLabel", "?")
                    score = msg.get("totalScore", 0)
                    driver = msg.get("driver", "?")
                    lap = msg.get("lapNumber", "?")
                    compound = msg.get("compound", "?")
                    tyre_life = msg.get("tyreLife", "?")
                    suggestion = msg.get("suggestion", "")

                    st.session_state.pit_strategy_alerts.append(
                        {
                            "Driver": driver,
                            "Lap": lap,
                            "Label": label,
                            "Score": (
                                f"{score:.1f}"
                                if isinstance(score, (int, float))
                                else str(score)
                            ),
                            "Compound": compound,
                            "Tyre": tyre_life,
                            "Reason": suggestion,
                        }
                    )
                    st.session_state.pit_strategy_alerts = bound_alerts(
                        st.session_state.pit_strategy_alerts
                    )

                elif "result" in msg:
                    driver = msg.get("driver", "?")
                    pit_lap = msg.get("pitLapNumber", "?")
                    result = msg.get("result", "?")
                    rival_ahead = msg.get("rivalAhead", "---")
                    rival_behind = msg.get("rivalBehind", "---")
                    gap_delta = msg.get("gapDeltaPct")
                    resolved_via = msg.get("resolvedVia", "?")

                    gap_str = "---"
                    if gap_delta is not None and isinstance(gap_delta, (int, float)):
                        gap_str = f"{gap_delta:.2f}%"

                    st.session_state.pit_eval_alerts.append(
                        {
                            "Driver": driver,
                            "Pit Lap": pit_lap,
                            "Result": result,
                            "Rival Ahead": rival_ahead if rival_ahead else "---",
                            "Rival Behind": rival_behind if rival_behind else "---",
                            "Gap Delta": gap_str,
                            "Via": resolved_via,
                        }
                    )
                    st.session_state.pit_eval_alerts = bound_alerts(
                        st.session_state.pit_eval_alerts
                    )

                elif "brakeDate" in msg:
                    driver = msg.get("driver", "?")
                    # compute coast duration from lift and brake iso timestamps
                    try:
                        lift_dt = pd.to_datetime(msg.get("liftDate"))
                        brake_dt = pd.to_datetime(msg.get("brakeDate"))
                        coast_s = f"{(brake_dt - lift_dt).total_seconds():.2f}"
                        time_str = brake_dt.strftime("%H:%M:%S")
                    except Exception:
                        coast_s = "?"
                        time_str = "?"
                    st.session_state.lift_coast_alerts.append(
                        {
                            "Driver": driver,
                            "Speed": msg.get("speed", "?"),
                            "Gear": msg.get("gear", "?"),
                            "Coast (s)": coast_s,
                            "Time": time_str,
                        }
                    )
                    st.session_state.lift_coast_alerts = bound_alerts(
                        st.session_state.lift_coast_alerts
                    )

                elif "delta" in msg:
                    driver = msg.get("driver", "?")
                    lap = msg.get("lapNumber", "?")
                    compound = msg.get("compound", "?")
                    tyre_life = msg.get("tyreLife", "?")
                    delta = msg.get("delta", 0)

                    st.session_state.tire_drop_alerts.append(
                        {
                            "Driver": driver,
                            "Lap": lap,
                            "Compound": compound,
                            "Tyre Life": tyre_life,
                            "Delta": (
                                f"+{delta:.3f}s"
                                if isinstance(delta, (int, float))
                                else str(delta)
                            ),
                        }
                    )
                    st.session_state.tire_drop_alerts = bound_alerts(
                        st.session_state.tire_drop_alerts
                    )

                elif "positionsLost" in msg:
                    st.session_state.drop_zone_alerts.append(
                        {
                            "Driver": msg.get("driver", "?"),
                            "Lap": msg.get("lapNumber", "?"),
                            "Emerge P": msg.get("emergencePosition", "?"),
                            "Lost": msg.get("positionsLost", "?"),
                        }
                    )
                    st.session_state.drop_zone_alerts = bound_alerts(
                        st.session_state.drop_zone_alerts
                    )

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
        df.index = df.index + 1
        leaderboard_placeholder.dataframe(df, use_container_width=True, hide_index=True)
    else:
        leaderboard_placeholder.info("Waiting for lap data...")

    # render pit strategy alerts (most recent first, with color-coded labels)
    if st.session_state.pit_strategy_alerts:
        recent = st.session_state.pit_strategy_alerts[::-1]
        strategy_df = pd.DataFrame(recent)
        styled = strategy_df.style.applymap(style_suggestion_label, subset=["Label"])
        pit_strategy_placeholder.dataframe(
            styled, use_container_width=True, hide_index=True
        )
    else:
        pit_strategy_placeholder.info("Waiting for pit strategy alerts...")

    # render pit stop evaluations (most recent first)
    if st.session_state.pit_eval_alerts:
        recent = st.session_state.pit_eval_alerts[::-1]
        eval_df = pd.DataFrame(recent)
        pit_eval_placeholder.dataframe(
            eval_df, use_container_width=True, hide_index=True
        )
    else:
        pit_eval_placeholder.info("Waiting for pit stop evaluations...")

    # render gap to leader evolution chart
    if st.session_state.gap_history:
        if st.session_state.leaderboard:
            sorted_board = sorted(
                st.session_state.leaderboard.values(),
                key=lambda x: x.get("Position") or 99,
            )
            top_drivers = [d["Driver"] for d in sorted_board[:3]]
        else:
            top_drivers = list(st.session_state.gap_history.keys())[:3]

        gap_data = {}
        for drv in top_drivers:
            if drv in st.session_state.gap_history:
                gap_data[drv] = st.session_state.gap_history[drv]

        if gap_data:
            gap_df = pd.DataFrame(gap_data)
            gap_df.index.name = "Lap"
            gap_df = gap_df.sort_index()
            if len(gap_df) > GAP_HISTORY_MAX_LAPS:
                gap_df = gap_df.iloc[-GAP_HISTORY_MAX_LAPS:]
            gap_chart_placeholder.line_chart(gap_df)
        else:
            gap_chart_placeholder.info("Waiting for gap data...")
    else:
        gap_chart_placeholder.info("Waiting for gap data...")

    # render tire age tracker (horizontal bar for top 5)
    if st.session_state.leaderboard:
        sorted_board = sorted(
            st.session_state.leaderboard.values(), key=lambda x: x.get("Position") or 99
        )
        top5 = sorted_board[:5]
        tire_drivers = []
        tire_lives = []
        for entry in top5:
            tl = entry.get("Tyre Life", 0)
            if tl == "---":
                tl = 0
            tire_drivers.append(entry["Driver"])
            tire_lives.append(int(tl))

        tire_df = pd.DataFrame(
            {"Tyre Life (laps)": tire_lives},
            index=tire_drivers,
        )
        tire_df.index.name = "Driver"
        tire_chart_placeholder.bar_chart(tire_df, horizontal=True)
    else:
        tire_chart_placeholder.info("Waiting for lap data...")

    # render tire drop alerts (most recent first)
    if st.session_state.tire_drop_alerts:
        recent = st.session_state.tire_drop_alerts[::-1]
        drop_df = pd.DataFrame(recent)
        tire_drop_placeholder.dataframe(
            drop_df, use_container_width=True, hide_index=True
        )
    else:
        tire_drop_placeholder.info("Waiting for tire degradation alerts...")

    # render lift & coast alerts (most recent first)
    if st.session_state.lift_coast_alerts:
        recent = st.session_state.lift_coast_alerts[::-1]
        lc_df = pd.DataFrame(recent)
        lift_coast_placeholder.dataframe(
            lc_df, use_container_width=True, hide_index=True
        )
    else:
        lift_coast_placeholder.info("Waiting for lift & coast alerts...")

    time.sleep(0.5)
