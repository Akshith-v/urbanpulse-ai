import pandas as pd
from sklearn.ensemble import IsolationForest


def compute_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["rolling_avg_aqi"] = df["aqi"].shift(1).rolling(5, min_periods=1).mean()
    df["std_dev"] = df["aqi"].shift(1).rolling(5, min_periods=1).std().fillna(0)

    z_scores = []

    for i in range(len(df)):
        std = max(float(df["std_dev"].iloc[i]), 5.0)
        mean = df["rolling_avg_aqi"].iloc[i]
        val = float(df["aqi"].iloc[i])

        if pd.isna(mean):
            z = 0.0
        else:
            z = (val - float(mean)) / std

        z_scores.append(z)

    df["z_score"] = z_scores
    return df


def compute_ml(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_cols = ["aqi", "temperature", "humidity", "feels_like", "wind_speed"]
    features = df[required_cols].copy()

    if len(df) > 10:
        train = features.iloc[:-1]
        test = features.iloc[-1:]

        model = IsolationForest(contamination=0.1, random_state=42)
        model.fit(train)

        score = float(model.decision_function(test)[0])
        flag = int(model.predict(test)[0])

        df["ml_score"] = 0.0
        df["ml_flag"] = 1

        df.loc[df.index[-1], "ml_score"] = score
        df.loc[df.index[-1], "ml_flag"] = flag
    else:
        df["ml_score"] = 0.0
        df["ml_flag"] = 1

    return df


def get_detection_type(z: float, ml_flag: int, ml_score: float) -> str:
    z_flag = abs(z) > 2.5
    ml_flag_bool = (ml_flag == -1) and (ml_score < -0.1)

    if z_flag and ml_flag_bool:
        return "both"
    if z_flag:
        return "statistical"
    if ml_flag_bool:
        return "ml"
    return "none"


def get_severity(z: float) -> str:
    z = abs(z)

    if z > 5:
        return "critical"
    if z > 3.5:
        return "high"
    if z > 2.5:
        return "medium"
    return "low"


def detect_anomaly(df: pd.DataFrame):
    if len(df) < 5:
        return None

    df = compute_stats(df)
    df = compute_ml(df)

    latest = df.iloc[-1]

    z = float(latest["z_score"])
    ml_score = float(latest["ml_score"])
    ml_flag = int(latest["ml_flag"])

    detection = get_detection_type(z, ml_flag, ml_score)

    if detection == "none":
        return None

    baseline = latest["rolling_avg_aqi"]
    if pd.isna(baseline):
        baseline = None
    else:
        baseline = float(baseline)

    return {
        "metric_name": "aqi",
        "value": float(latest["aqi"]),
        "baseline": baseline,
        "z_score": float(z),
        "ml_score": float(ml_score),
        "detection_type": detection,
        "severity": get_severity(z),
        "source_timestamp": int(latest["source_timestamp"]),
    }