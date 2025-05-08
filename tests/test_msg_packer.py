import pandas as pd

from data_agent.msg_packer import decode_payload, encode_dataframe


def test_encode_decode_roundtrip(test_dataframe):
    # Encode the DataFrame into compressed Msgpack payload
    payload = encode_dataframe(test_dataframe)
    assert isinstance(payload, (bytes, bytearray)), "Payload should be bytes"

    # Decode the payload back into a DataFrame
    df_decoded = decode_payload(payload)

    # Check content equality
    pd.testing.assert_frame_equal(df_decoded, test_dataframe, check_freq=False)

    payload = encode_dataframe(test_dataframe, ts_unit="ns", zstd_level=5)
    assert isinstance(payload, (bytes, bytearray)), "Payload should be bytes"

    # Decode the payload back into a DataFrame
    df_decoded = decode_payload(payload)

    # Check content equality
    pd.testing.assert_frame_equal(df_decoded, test_dataframe, check_freq=False)


def test_metadata_preserved(test_dataframe):
    payload = encode_dataframe(test_dataframe, ts_unit="ms", zstd_level=3)
    df_decoded = decode_payload(payload)
    # Index frequency and dtype
    assert test_dataframe.index.equals(
        test_dataframe.index
    ), "Index should be preserved"
    # Column names
    assert list(df_decoded.columns) == list(
        test_dataframe.columns
    ), "Columns should be preserved"
    # Dtypes
    for col in test_dataframe.columns:
        assert (
            df_decoded[col].dtype == test_dataframe[col].dtype
        ), f"Column {col} dtype should be preserved"
