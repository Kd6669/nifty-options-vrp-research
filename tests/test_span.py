from __future__ import annotations

from datetime import date, datetime, timezone
import tempfile
import unittest
import zipfile
from pathlib import Path

from nifty_span.span import (
    SpanContract,
    SpanData,
    SpanMarginError,
    SpanParquetReader,
    extract_span_archives,
    margin_for_candidate_legs,
    parse_span_zip,
    span_day_status,
)
from nifty_span.span.contracts import slot_fallback_order


class SpanTests(unittest.TestCase):
    def test_parse_span_zip_keeps_nifty_and_extracts_scenarios(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "nsccl.20250102.i1.zip"
            _write_span_zip(zip_path)

            rows = parse_span_zip(zip_path, symbols_filter=("NIFTY",))

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["date"], date(2025, 1, 2))
        self.assertEqual(row["time_slot"], "BOD")
        self.assertEqual(row["symbol"], "NIFTY")
        self.assertEqual(row["instrument"], "CE")
        self.assertEqual(row["s16"], 16.0)

    def test_extract_and_read_span_parquet(self) -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow is not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_dir = root / "raw"
            day_dir = raw_dir / "2025" / "01" / "02"
            day_dir.mkdir(parents=True)
            _write_span_zip(day_dir / "nsccl.20250102.i1.zip")
            parquet_dir = root / "parquet"

            report = extract_span_archives(
                span_data_dir=raw_dir,
                parquet_dir=parquet_dir,
                symbols_filter=("NIFTY",),
                trading_date=date(2025, 1, 2),
                max_workers=1,
            )
            data = SpanParquetReader.load(parquet_dir, date(2025, 1, 2), time_slot="BOD")
            status = span_day_status(
                trading_date=date(2025, 1, 2),
                parquet_dir=parquet_dir,
                raw_root=raw_dir,
            )

        self.assertTrue(report.ok)
        self.assertEqual(len(data), 1)
        self.assertTrue(status.ready)
        self.assertIsNotNone(data.lookup_option("NIFTY", "CE", "2025-01-09", 24000.0))

    def test_latest_span_slot_selects_newest_available_intraday_slot(self) -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow is not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_dir = root / "raw"
            day_dir = raw_dir / "2025" / "01" / "02"
            day_dir.mkdir(parents=True)
            _write_span_zip(day_dir / "nsccl.20250102.i1.zip")
            _write_span_zip(day_dir / "nsccl.20250102.i3.zip")
            parquet_dir = root / "parquet"

            extract_span_archives(
                span_data_dir=raw_dir,
                parquet_dir=parquet_dir,
                symbols_filter=("NIFTY",),
                trading_date=date(2025, 1, 2),
                max_workers=1,
            )
            latest = SpanParquetReader.load(parquet_dir, date(2025, 1, 2), time_slot="LATEST")
            status = span_day_status(
                trading_date=date(2025, 1, 2),
                parquet_dir=parquet_dir,
                raw_root=raw_dir,
                preferred_time_slot="S3",
            )
            missing = span_day_status(
                trading_date=date(2025, 1, 2),
                parquet_dir=parquet_dir,
                raw_root=raw_dir,
                preferred_time_slot="ID4",
            )

        self.assertEqual(latest.selected_time_slot, "ID2")
        self.assertTrue(status.ready)
        self.assertEqual(status.requested_time_slot, "ID2")
        self.assertEqual(status.selected_time_slot, "ID2")
        self.assertFalse(missing.ready)
        self.assertEqual(slot_fallback_order("ID4"), ("ID4",))

    def test_model_a_margin_requires_exact_contract(self) -> None:
        span_data = SpanData(
            {("NIFTY", "CE", date(2025, 1, 9), 24000.0): SpanContract(tuple(-10.0 for _ in range(16)))},
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "SELL",
                    "option_type": "CE",
                    "strike": 24000.0,
                    "lot_size": 25,
                    "limit_price": 100.0,
                    "qty_ratio": 1,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-01-09",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )
        self.assertEqual(margin.source, "span_model_a")
        self.assertGreater(margin.margin, 0.0)

        with self.assertRaises(SpanMarginError):
            margin_for_candidate_legs(
                legs=(
                    {
                        "side": "SELL",
                        "option_type": "PE",
                        "strike": 24000.0,
                        "lot_size": 25,
                        "limit_price": 100.0,
                    },
                ),
                span_data=span_data,
                index="NIFTY",
                expiry="2025-01-09",
                spot=24000.0,
            )

    def test_model_a_long_option_is_premium_only_after_net_option_value(self) -> None:
        span_data = SpanData(
            {("NIFTY", "CE", date(2025, 1, 9), 24000.0): SpanContract(tuple(200.0 for _ in range(16)), price=100.0)},
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "BUY",
                    "option_type": "CE",
                    "strike": 24000.0,
                    "lot_size": 25,
                    "limit_price": 100.0,
                    "qty_ratio": 1,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-01-09",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.m_span, 5000.0)
        self.assertEqual(margin.s_net_clamped, 0.0)
        self.assertEqual(margin.long_premium, 2500.0)
        self.assertEqual(margin.long_option_value, 2500.0)
        self.assertEqual(margin.net_option_value, 2500.0)
        self.assertEqual(margin.margin, 2500.0)

    def test_model_a_uses_span_option_values_for_nov_and_live_price_for_cash_premium(self) -> None:
        span_data = SpanData(
            {
                ("NIFTY", "CE", date(2025, 1, 9), 24000.0): SpanContract(tuple(40.0 for _ in range(16)), price=80.0),
                ("NIFTY", "CE", date(2025, 1, 9), 24200.0): SpanContract(tuple(-30.0 for _ in range(16)), price=60.0),
            },
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "BUY",
                    "option_type": "CE",
                    "strike": 24000.0,
                    "lot_size": 25,
                    "limit_price": 100.0,
                    "qty_ratio": 1,
                },
                {
                    "side": "SELL",
                    "option_type": "CE",
                    "strike": 24200.0,
                    "lot_size": 25,
                    "limit_price": 40.0,
                    "qty_ratio": 1,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-01-09",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.m_span, 1750.0)
        self.assertEqual(margin.credit_sum, 1500.0)
        self.assertEqual(margin.long_premium, 2500.0)
        self.assertEqual(margin.long_option_value, 2000.0)
        self.assertEqual(margin.net_option_value, 500.0)
        self.assertEqual(margin.s_net_clamped, 1250.0)
        self.assertEqual(margin.margin, 15750.0)

    def test_model_a_applies_short_index_expiry_day_extra_elm(self) -> None:
        span_data = SpanData(
            {("NIFTY", "CE", date(2025, 1, 2), 24000.0): SpanContract(tuple(-10.0 for _ in range(16)), price=100.0)},
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "SELL",
                    "option_type": "CE",
                    "expiry": "2025-01-02",
                    "strike": 24000.0,
                    "lot_size": 25,
                    "limit_price": 100.0,
                    "qty_ratio": 1,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-01-02",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.elm_required, 24000.0)

    def test_model_a_applies_long_maturity_short_index_option_elm(self) -> None:
        span_data = SpanData(
            {("NIFTY", "CE", date(2025, 11, 1), 24000.0): SpanContract(tuple(-10.0 for _ in range(16)), price=100.0)},
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "SELL",
                    "option_type": "CE",
                    "expiry": "2025-11-01",
                    "strike": 24000.0,
                    "lot_size": 25,
                    "limit_price": 100.0,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-11-01",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.elm_required, 30000.0)

    def test_model_a_applies_deep_otm_stock_short_option_elm(self) -> None:
        span_data = SpanData(
            {("RELIANCE", "CE", date(2025, 1, 30), 1400.0): SpanContract(tuple(-1.0 for _ in range(16)), price=10.0)},
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "SELL",
                    "option_type": "CE",
                    "expiry": "2025-01-30",
                    "strike": 1400.0,
                    "lot_size": 100,
                    "limit_price": 10.0,
                },
            ),
            span_data=span_data,
            index="RELIANCE",
            expiry="2025-01-30",
            spot=1000.0,
            prev_close_spot=1000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.elm_required, 5250.0)

    def test_model_a_reduces_elm_for_eligible_index_futures_calendar_spread(self) -> None:
        span_data = SpanData(
            {
                ("NIFTY", "FUT", date(2025, 1, 30), 0.0): SpanContract(tuple(-10.0 for _ in range(16)), price=24000.0),
                ("NIFTY", "FUT", date(2025, 2, 27), 0.0): SpanContract(tuple(10.0 for _ in range(16)), price=24150.0),
            },
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "SELL",
                    "instrument": "FUT",
                    "option_type": "FUT",
                    "expiry": "2025-01-30",
                    "lot_size": 25,
                    "limit_price": 24000.0,
                },
                {
                    "side": "BUY",
                    "instrument": "FUT",
                    "option_type": "FUT",
                    "expiry": "2025-02-27",
                    "lot_size": 25,
                    "limit_price": 24150.0,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-01-30",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.elm_required, 4025.0)

    def test_model_a_does_not_apply_index_calendar_spread_on_near_expiry_day(self) -> None:
        span_data = SpanData(
            {
                ("NIFTY", "FUT", date(2025, 1, 30), 0.0): SpanContract(tuple(-10.0 for _ in range(16)), price=24000.0),
                ("NIFTY", "FUT", date(2025, 2, 27), 0.0): SpanContract(tuple(10.0 for _ in range(16)), price=24150.0),
            },
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 30),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "SELL",
                    "instrument": "FUT",
                    "option_type": "FUT",
                    "expiry": "2025-01-30",
                    "lot_size": 25,
                    "limit_price": 24000.0,
                },
                {
                    "side": "BUY",
                    "instrument": "FUT",
                    "option_type": "FUT",
                    "expiry": "2025-02-27",
                    "lot_size": 25,
                    "limit_price": 24150.0,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-01-30",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 30, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.elm_required, 24075.0)

    def test_model_a_includes_external_delivery_crystallized_cross_margin_and_floor_hooks(self) -> None:
        span_data = SpanData(
            {("NIFTY", "CE", date(2025, 1, 9), 24000.0): SpanContract(tuple(-10.0 for _ in range(16)), price=100.0)},
            selected_time_slot="BOD",
            trading_date=date(2025, 1, 2),
        )

        margin = margin_for_candidate_legs(
            legs=(
                {
                    "side": "SELL",
                    "option_type": "CE",
                    "expiry": "2025-01-09",
                    "strike": 24000.0,
                    "lot_size": 25,
                    "limit_price": 100.0,
                    "additional_margin": 1000.0,
                    "additional_margin_rate": 0.01,
                    "delivery_margin": 2000.0,
                    "crystallized_obligation_margin": 3000.0,
                    "cross_margin_benefit": 500.0,
                    "minimum_total_margin_rate": 0.25,
                },
            ),
            span_data=span_data,
            index="NIFTY",
            expiry="2025-01-09",
            spot=24000.0,
            eval_dt=datetime(2025, 1, 2, 9, 20, tzinfo=timezone.utc),
        )

        self.assertEqual(margin.add_on_margin, 7000.0)
        self.assertEqual(margin.delivery_margin, 2000.0)
        self.assertEqual(margin.crystallized_obligation_margin, 3000.0)
        self.assertEqual(margin.cross_margin_benefit, 500.0)
        self.assertEqual(margin.minimum_total_margin_floor, 150000.0)
        self.assertEqual(margin.margin, 150000.0)


def _write_span_zip(path: Path) -> None:
    xml = """
<root>
  <oopPf>
    <pfCode>NIFTY</pfCode>
    <cvf>1</cvf>
    <series>
      <pe>20250109</pe>
      <cvf>1</cvf>
      <scanRate><priceScan>100</priceScan><volScan>10</volScan></scanRate>
      <opt>
        <o>C</o>
        <k>24000</k>
        <p>100.5</p>
        <d>0.5</d>
        <v>0.18</v>
        <ra>
          <a>1</a><a>2</a><a>3</a><a>4</a>
          <a>5</a><a>6</a><a>7</a><a>8</a>
          <a>9</a><a>10</a><a>11</a><a>12</a>
          <a>13</a><a>14</a><a>15</a><a>16</a>
          <d>0.5</d>
        </ra>
      </opt>
    </series>
  </oopPf>
</root>
""".strip()
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("nsccl.20250102.i1.spn", xml)


if __name__ == "__main__":
    unittest.main()
