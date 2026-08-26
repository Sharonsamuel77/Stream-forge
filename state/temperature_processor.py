from datetime import datetime, timedelta


class TemperatureProcessor:
    """
    Calculate a true 5-minute sliding/rolling temperature
    average for each truck.

    Every new reading:
    1. Is added to the truck's reading history.
    2. Readings older than 5 minutes are removed.
    3. The average of the remaining readings is calculated.
    4. The updated window is persisted in RocksDB.
    """

    WINDOW_MINUTES = 5

    def __init__(self, state_store):
        self.state_store = state_store

    def process(self, truck_id, temperature, timestamp):
        """
        Process one telemetry reading.

        Returns the current 5-minute rolling average.
        """

        event_time = datetime.fromisoformat(timestamp)

        state_key = f"truck:{truck_id}"

        # ----------------------------------------------------
        # Load existing state
        # ----------------------------------------------------

        state = self.state_store.get(state_key)

        if state is None:
            state = {
                "readings": []
            }

        # ----------------------------------------------------
        # Handle old state format safely
        # ----------------------------------------------------
        #
        # Previous version stored:
        #
        # window_start
        # temperature_sum
        # reading_count
        #
        # That format cannot be used for a sliding window.
        # Start a fresh rolling-window state if encountered.
        #

        if "readings" not in state:
            state = {
                "readings": []
            }

        readings = state["readings"]

        # ----------------------------------------------------
        # Add current reading
        # ----------------------------------------------------

        readings.append(
            {
                "timestamp": timestamp,
                "temperature": float(temperature),
            }
        )

        # ----------------------------------------------------
        # Calculate 5-minute cutoff
        # ----------------------------------------------------

        cutoff_time = (
            event_time
            - timedelta(
                minutes=self.WINDOW_MINUTES
            )
        )

        # ----------------------------------------------------
        # Remove readings older than 5 minutes
        # ----------------------------------------------------

        valid_readings = []

        for reading in readings:

            try:

                reading_time = datetime.fromisoformat(
                    reading["timestamp"]
                )

                if reading_time >= cutoff_time:
                    valid_readings.append(
                        reading
                    )

            except (
                ValueError,
                KeyError,
                TypeError,
            ):
                # Ignore malformed historical readings.
                continue

        readings = valid_readings

        # ----------------------------------------------------
        # Calculate rolling average
        # ----------------------------------------------------

        temperature_sum = sum(
            float(
                reading["temperature"]
            )
            for reading in readings
        )

        reading_count = len(
            readings
        )

        average_temperature = (
            temperature_sum / reading_count
            if reading_count > 0
            else 0.0
        )

        # ----------------------------------------------------
        # Determine actual window boundaries
        # ----------------------------------------------------

        if readings:

            window_start = readings[0][
                "timestamp"
            ]

            window_end = readings[-1][
                "timestamp"
            ]

        else:

            window_start = timestamp
            window_end = timestamp

        # ----------------------------------------------------
        # Save updated sliding-window state
        # ----------------------------------------------------

        state = {
            "readings": readings,
            "window_start": window_start,
            "window_end": window_end,
            "temperature_sum": temperature_sum,
            "reading_count": reading_count,
            "average_temperature": round(
                average_temperature,
                2,
            ),
        }

        self.state_store.put(
            state_key,
            state
        )

        # ----------------------------------------------------
        # Return rolling-window result
        # ----------------------------------------------------

        return {
            "truck_id": truck_id,
            "window_start": window_start,
            "window_end": window_end,
            "average_temperature": round(
                average_temperature,
                2,
            ),
            "reading_count": reading_count,
        }