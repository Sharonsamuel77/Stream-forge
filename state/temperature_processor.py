from datetime import datetime, timedelta


class TemperatureProcessor:
    """Calculate a 5-minute temperature average for each truck."""

    WINDOW_MINUTES = 5

    def __init__(self, state_store):
        self.state_store = state_store

    def process(self, truck_id, temperature, timestamp):
        event_time = datetime.fromisoformat(timestamp)

        state_key = f"truck:{truck_id}"

        state = self.state_store.get(state_key)

        if state is None:
            state = {
                "window_start": timestamp,
                "temperature_sum": 0.0,
                "reading_count": 0,
            }

        window_start = datetime.fromisoformat(
            state["window_start"]
        )

        if event_time - window_start >= timedelta(
            minutes=self.WINDOW_MINUTES
        ):

            average = (
                state["temperature_sum"]
                / state["reading_count"]
                if state["reading_count"] > 0
                else 0.0
            )

            result = {
                "truck_id": truck_id,
                "window_start": state["window_start"],
                "window_end": timestamp,
                "average_temperature": round(
                    average,
                    2
                ),
            }

            state = {
                "window_start": timestamp,
                "temperature_sum": 0.0,
                "reading_count": 0,
            }

        else:
            result = None

        state["temperature_sum"] += temperature
        state["reading_count"] += 1

        self.state_store.put(
            state_key,
            state
        )

        return result