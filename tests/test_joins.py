from __future__ import annotations

import unittest

from dhan_data_fetch_stream.joins import (
    JOIN_BACKWARD,
    JOIN_DUPLICATE_SPOT_KEY,
    JOIN_EXACT,
    JOIN_FUTURE_FORBIDDEN,
    JOIN_NO_SPOT,
    JOIN_OUTSIDE_TOLERANCE,
    join_options_to_india_vix,
    join_options_to_spot,
)


class SpotJoinTests(unittest.TestCase):
    def test_vix_join_is_separate_and_never_crosses_date(self) -> None:
        options = [
            {"timestamp_ist": "2026-07-15T09:16:00+05:30", "trade_date": "2026-07-15"}
        ]
        vix = [
            {
                "timestamp_ist": "2026-07-14T15:30:00+05:30",
                "trade_date": "2026-07-14",
                "close": 13.5,
            }
        ]
        result = join_options_to_india_vix(options, vix)
        self.assertIsNone(result[0]["india_vix"])
        self.assertEqual(result[0]["india_vix_join_status"], JOIN_NO_SPOT)
        self.assertNotIn("joined_spot", result[0])

    def test_exact_then_backward_join_without_mutation(self) -> None:
        options = [
            {"timestamp": "2026-07-14T09:16:00+05:30", "close": 100.0},
            {"timestamp": "2026-07-14T09:16:45+05:30", "close": 101.0},
        ]
        spots = [{"timestamp": "2026-07-14T09:16:00+05:30", "spot": 25001.0}]

        joined = join_options_to_spot(options, spots)

        self.assertEqual(joined[0]["spot_join_status"], JOIN_EXACT)
        self.assertEqual(joined[1]["spot_join_status"], JOIN_BACKWARD)
        self.assertEqual(joined[1]["spot_join_lag_seconds"], 45.0)
        self.assertEqual(joined[1]["joined_spot"], 25001.0)
        self.assertNotIn("joined_spot", options[0])

    def test_future_spot_is_never_used(self) -> None:
        joined = join_options_to_spot(
            [{"timestamp": "2026-07-14T09:16:00+05:30"}],
            [{"timestamp": "2026-07-14T09:16:01+05:30", "spot": 25001.0}],
        )

        self.assertEqual(joined[0]["spot_join_status"], JOIN_FUTURE_FORBIDDEN)
        self.assertIsNone(joined[0]["joined_spot"])

    def test_more_than_60_seconds_is_rejected(self) -> None:
        joined = join_options_to_spot(
            [{"timestamp": "2026-07-14T09:17:01+05:30"}],
            [{"timestamp": "2026-07-14T09:16:00+05:30", "spot": 25001.0}],
        )

        self.assertEqual(joined[0]["spot_join_status"], JOIN_OUTSIDE_TOLERANCE)
        self.assertIsNone(joined[0]["joined_spot"])

    def test_no_cross_date_or_explicit_session_join(self) -> None:
        joined_dates = join_options_to_spot(
            [{"timestamp": "2026-07-15T09:15:00+05:30"}],
            [{"timestamp": "2026-07-14T15:30:00+05:30", "spot": 25001.0}],
        )
        joined_sessions = join_options_to_spot(
            [{"timestamp": "2026-07-14T09:16:00+05:30", "session": "B"}],
            [{"timestamp": "2026-07-14T09:16:00+05:30", "session": "A", "spot": 25001.0}],
        )

        self.assertEqual(joined_dates[0]["spot_join_status"], JOIN_NO_SPOT)
        self.assertEqual(joined_sessions[0]["spot_join_status"], JOIN_NO_SPOT)

    def test_repeated_explicit_session_id_cannot_cross_ist_trade_date(self) -> None:
        joined = join_options_to_spot(
            [{"timestamp": "2026-07-15T09:15:00+05:30", "session": "REGULAR"}],
            [{"timestamp": "2026-07-14T09:15:00+05:30", "session": "REGULAR", "spot": 25001.0}],
        )

        self.assertEqual(joined[0]["spot_join_status"], JOIN_NO_SPOT)

    def test_duplicate_spot_timestamp_fails_explicitly(self) -> None:
        joined = join_options_to_spot(
            [{"timestamp": "2026-07-14T09:16:00+05:30"}],
            [
                {"timestamp": "2026-07-14T09:16:00+05:30", "spot": 25001.0},
                {"timestamp": "2026-07-14T09:16:00+05:30", "spot": 25002.0},
            ],
        )

        self.assertEqual(joined[0]["spot_join_status"], JOIN_DUPLICATE_SPOT_KEY)
        self.assertIsNone(joined[0]["joined_spot"])

    def test_rejects_tolerance_above_contract(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 0 and 60"):
            join_options_to_spot([], [], tolerance_seconds=61.0)


if __name__ == "__main__":
    unittest.main()
