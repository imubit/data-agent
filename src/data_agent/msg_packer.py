import msgpack
import numpy as np
import pandas as pd
import zstandard as zstd


def encode_dataframe(df, ts_unit="s", zstd_level=10):
    # 1) timestamps
    ts = df.index.view("int64")
    if ts_unit != "ns":
        factor = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[ts_unit]
        ts = (ts // factor).astype("int64")
    ts_blob = ts.tobytes()

    # 2) split fixed vs. object columns
    obj_cols = df.select_dtypes(include=["object"]).columns.tolist()
    num_cols = [c for c in df.columns if c not in obj_cols]

    # 2a) fixed‐dtype blob
    if num_cols:
        rec = df[num_cols].to_records(index=False)
        num_blob = rec.tobytes()
        num_descr = rec.dtype.descr
    else:
        num_blob = b""
        num_descr = []

    # 2b) object‐dtype data (simple Python lists)
    obj_data = {c: df[c].tolist() for c in obj_cols}

    # 3) pack into ExtTypes + one metadata map
    p = msgpack.Packer(use_bin_type=True, strict_types=True)
    parts = [
        p.pack(msgpack.ExtType(0, ts_blob)),
        p.pack(msgpack.ExtType(1, num_blob)),
        # Ext code 2 carries the already‐msgpacked object data blob:
        p.pack(msgpack.ExtType(2, msgpack.packb(obj_data, use_bin_type=True))),
    ]

    # build metadata — now include the original columns order
    meta = {
        "ts_unit": ts_unit,
        "num_descr": [list(x) for x in num_descr],
        "num_cols": num_cols,
        "obj_cols": obj_cols,
        "orig_cols": df.columns.tolist(),
        "index_name": df.index.name,
    }

    parts.append(p.pack(meta))
    raw = b"".join(parts)
    return zstd.ZstdCompressor(level=zstd_level).compress(raw)


def decode_payload(blob):
    # 1) decompress
    raw = zstd.ZstdDecompressor().decompress(blob)

    # 2) ext_hook to pull out our three ExtTypes
    def ext_hook(code, data):
        if code == 0:
            # timestamps
            return np.frombuffer(data, dtype="int64")
        if code == 1:
            # numeric blob
            return data
        if code == 2:
            # object blob
            return data
        return msgpack.ExtType(code, data)

    # 3) unpack in sequence
    unpacker = msgpack.Unpacker(ext_hook=ext_hook, raw=False)
    unpacker.feed(raw)
    ts_arr = next(unpacker)
    num_blob = next(unpacker)
    obj_blob = next(unpacker)
    meta = next(unpacker)

    # 4) rebuild timestamps
    factor = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[
        meta["ts_unit"]
    ]
    idx = pd.to_datetime(ts_arr * factor)
    idx.name = meta["index_name"]

    # 5) rebuild fixed‐dtype DataFrame
    num_cols = meta["num_cols"]
    if num_cols:
        dtype_descr = [tuple(x) for x in meta["num_descr"]]
        rec = np.frombuffer(num_blob, dtype=np.dtype(dtype_descr))
        df_num = pd.DataFrame(rec, columns=num_cols)
    else:
        df_num = pd.DataFrame(index=idx)

    # 6) rebuild object‐dtype DataFrame
    obj_cols = meta["obj_cols"]
    if obj_cols:
        obj_data = msgpack.unpackb(obj_blob, raw=False)
        df_obj = pd.DataFrame(obj_data)
    else:
        df_obj = pd.DataFrame()

    # 7) combine, restore index, and **reorder**:
    df = pd.concat([df_num, df_obj], axis=1)
    df.index = idx

    # ← HERE: reorder exactly as original
    df = df[meta["orig_cols"]]

    return df
