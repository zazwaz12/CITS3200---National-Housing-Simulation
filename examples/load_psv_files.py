from context import nhs
import polars as pl

# aliases
read_psv = nhs.data.read_psv
read_all_psv = nhs.data.read_all_psv
pmap = nhs.utils.parallel.pmap

lazy_frames = read_all_psv(
    "./DataFiles/FilesIn/Standard/", "./DataFiles/FilesIn/Standard/{key}_psv.psv"
)


def psv_info(name_frame: tuple[str, pl.LazyFrame | None]) -> tuple | None:
    name, frame = name_frame
    if frame is None:
        print(f"Failed to read {name}")
        return
    head = frame.head().collect()
    n_cols = frame.collect_schema().len()
    n_rows = (
        frame.with_columns(frame.collect_schema().names()[0])
        .select(pl.len())
        .collect()
        .item()
    )
    return name, n_cols, n_rows, head


[print(i) for i in pmap(psv_info, lazy_frames.items(), n_workers=5)]
