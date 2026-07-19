from __future__ import annotations

import base64
import json
from pathlib import Path
import tempfile
import unittest

from dhan_data_fetch_stream.core import (
    DhanCredentials,
    DhanInstrumentCsvLoader,
    PacketParquetWriter,
    dhan_expiry_instruments,
    dhan_feed_packet_payload,
    dhan_intraday_rows,
    dhan_option_chain_rows,
    dhan_option_chain_spot,
    _redact_token_text,
)
from dhan_data_fetch_stream.core import InstrumentIdentity, InstrumentMaster


class CoreTests(unittest.TestCase):
    def test_redact_token_text_removes_header_values_and_jwts(self) -> None:
        raw = (
            'access-token: secret-value '
            'authorization="Bearer abc" '
            'eyJhbGciOiJIUzI1NiJ9.eyJkaGFuQ2xpZW50SWQiOiIxIn0.signature'
        )

        redacted = _redact_token_text(raw)

        self.assertNotIn("secret-value", redacted)
        self.assertNotIn("Bearer abc", redacted)
        self.assertNotIn("eyJhbGci", redacted)
        self.assertIn("<redacted>", redacted)
        self.assertIn("<redacted-jwt>", redacted)

    def test_credentials_infer_client_id_from_jwt_payload(self) -> None:
        token = _jwt_with_payload({"dhanClientId": "1110995204"})

        credentials = DhanCredentials.from_env({"DHAN_ACCESS_TOKEN": token})

        self.assertEqual(credentials.client_id, "1110995204")
        self.assertEqual(credentials.access_token, token)

    def test_instrument_loader_filters_nifty_options(self) -> None:
        master = DhanInstrumentCsvLoader().load_from_text(
            _dhan_csv(),
            indices=("NIFTY",),
            expiries=("2026-06-30",),
        )

        instruments = dhan_expiry_instruments(master, index="NIFTY", expiry="2026-06-30", exchange="NSE")

        self.assertEqual([item.instrument_token for item in instruments], ["35191", "35192"])
        self.assertEqual(instruments[0].segment, "NSE_FNO")
        self.assertEqual(instruments[0].lot_size, 65)

    def test_option_chain_rows_normalize_dhan_contracts(self) -> None:
        rows = dhan_option_chain_rows(
            _option_chain_response(),
            index="NIFTY",
            expiry="2026-06-30",
            exchange="NSE",
            instrument_master=_master(),
            received_ts="2026-06-25T04:00:00+00:00",
        )

        self.assertEqual(dhan_option_chain_spot(_option_chain_response()), 25642.8)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["provider"], "dhan_rest")
        self.assertEqual(rows[0]["instrument_token"], "42528")
        self.assertEqual(rows[0]["symbol"], "NIFTY-Jun2026-25650-CE")
        self.assertEqual(rows[0]["bid"], 133.55)
        self.assertEqual(rows[0]["ask"], 134.0)
        self.assertEqual(rows[0]["oi"], 3786445)
        self.assertEqual(rows[0]["delta"], 0.53871)

    def test_intraday_rows_flatten_dhan_arrays_with_identity(self) -> None:
        identity = _master().lookup(index="NIFTY", expiry="2026-06-30", strike=25650.0, option_type="CE")

        rows = dhan_intraday_rows(
            {
                "timestamp": [1782359160.0, 1782359220.0],
                "open": [88.2, 84.95],
                "high": [88.4, 85.9],
                "low": [83.25, 81.55],
                "close": [84.25, 82.95],
                "volume": [1290510.0, 900185.0],
                "open_interest": [6406595.0, 6752005.0],
            },
            identity=identity,
            trading_date="2026-06-25",
            exchange_segment="NSE_FNO",
            instrument="OPTIDX",
            interval="1",
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["provider"], "dhan_historical")
        self.assertEqual(rows[0]["timestamp_ist"], "2026-06-25T09:16:00+05:30")
        self.assertEqual(rows[0]["volume"], 1290510)
        self.assertEqual(rows[0]["open_interest"], 6406595)

    def test_feed_packet_payload_attaches_identity_and_depth_top(self) -> None:
        payload = dhan_feed_packet_payload(
            {
                "type": "Full Data",
                "exchange_segment": 2,
                "security_id": 42528,
                "LTP": "134.00",
                "LTT": "09:15:01",
                "volume": 117567970,
                "OI": 3786445,
                "depth": [
                    {
                        "bid_quantity": 1625,
                        "ask_quantity": 1365,
                        "bid_price": "133.55",
                        "ask_price": "134.00",
                    }
                ],
            },
            identity_by_security={"42528": _master().lookup(index="NIFTY", expiry="2026-06-30", strike=25650.0, option_type="CE")},
            index="NIFTY",
            expiry="2026-06-30",
            exchange="NSE",
            received_ts="2026-06-25T04:00:00+00:00",
        )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["provider"], "dhan_feed")
        self.assertEqual(payload["exchange_segment_name"], "NSE_FNO")
        self.assertEqual(payload["symbol"], "NIFTY-Jun2026-25650-CE")
        self.assertEqual(payload["bid"], 133.55)
        self.assertEqual(payload["ask"], 134.0)
        self.assertEqual(payload["ltp"], 134.0)

    def test_packet_parquet_writer_writes_payload_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            writer = PacketParquetWriter(Path(tmp), prefix="packets", flush_rows=1)
            writer.append_packet(
                payload={"provider": "dhan_rest", "symbol": "NIFTY", "ltp": 100.5},
                packet_kind="quote",
                stream="stream:test",
                stream_id="1-0",
            )
            writer.close()
            rows = _read_parquet_rows(writer.written_files[0])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["provider"], "dhan_rest")
        self.assertIn('"symbol":"NIFTY"', rows[0]["payload_json"])


def _jwt_with_payload(payload: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
    return f"header.{encoded}.signature"


def _dhan_csv() -> str:
    return "\n".join(
        [
            "SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_SMST_SECURITY_ID,SEM_INSTRUMENT_NAME,SEM_EXPIRY_CODE,SEM_TRADING_SYMBOL,SEM_LOT_UNITS,SEM_CUSTOM_SYMBOL,SEM_EXPIRY_DATE,SEM_STRIKE_PRICE,SEM_OPTION_TYPE,SEM_TICK_SIZE,SEM_EXPIRY_FLAG,SEM_EXCH_INSTRUMENT_TYPE,SEM_SERIES,SM_SYMBOL_NAME",
            "NSE,D,35191,OPTIDX,0,NIFTY-Jun2026-27650-CE,65.0,NIFTY 30 JUN 27650 CALL,2026-06-30 14:30:00,27650.00000,CE,5.0000,M,OP,,",
            "NSE,D,35192,OPTIDX,0,NIFTY-Jun2026-27650-PE,65.0,NIFTY 30 JUN 27650 PUT,2026-06-30 14:30:00,27650.00000,PE,5.0000,M,OP,,",
            "NSE,D,35000,OPTIDX,0,BANKNIFTY-Jun2026-65400-CE,30.0,BANKNIFTY 30 JUN 65400 CALL,2026-06-30 14:30:00,65400.00000,CE,5.0000,M,OP,,",
        ]
    )


def _option_chain_response() -> dict[str, object]:
    return {
        "data": {
            "last_price": 25642.8,
            "oc": {
                "25650.000000": {
                    "ce": {
                        "greeks": {"delta": 0.53871, "theta": -15.1539, "gamma": 0.00132, "vega": 12.18593},
                        "implied_volatility": 9.789193798280868,
                        "last_price": 134,
                        "oi": 3786445,
                        "security_id": 42528,
                        "top_ask_price": 134,
                        "top_ask_quantity": 1365,
                        "top_bid_price": 133.55,
                        "top_bid_quantity": 1625,
                        "volume": 117567970,
                    },
                    "pe": {
                        "greeks": {"delta": -0.46732, "theta": -10.61131, "gamma": 0.00109, "vega": 12.2025},
                        "implied_volatility": 11.939337251984934,
                        "last_price": 132.8,
                        "oi": 3096145,
                        "security_id": 42529,
                        "top_ask_price": 132.75,
                        "top_ask_quantity": 390,
                        "top_bid_price": 132.45,
                        "top_bid_quantity": 65,
                        "volume": 157009970,
                    },
                }
            },
        },
        "status": "success",
    }


def _master() -> InstrumentMaster:
    return InstrumentMaster.from_rows(
        [
            InstrumentIdentity(
                index="NIFTY",
                expiry="2026-06-30",
                strike=25650.0,
                option_type="CE",
                instrument_token="42528",
                symbol="NIFTY-Jun2026-25650-CE",
                lot_size=65,
                exchange="NSE",
                segment="NSE_FNO",
                exchange_token="42528",
            ),
            InstrumentIdentity(
                index="NIFTY",
                expiry="2026-06-30",
                strike=25650.0,
                option_type="PE",
                instrument_token="42529",
                symbol="NIFTY-Jun2026-25650-PE",
                lot_size=65,
                exchange="NSE",
                segment="NSE_FNO",
                exchange_token="42529",
            ),
        ]
    )


def _read_parquet_rows(path: Path) -> list[dict[str, object]]:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    return pq.read_table(path).to_pylist()
