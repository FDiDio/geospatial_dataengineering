import os

import h3
import pandas as pd

from utils import h3_processor
from utils.h3_processor import enrich_with_h3, lookup_file_path, precompute_h3_index_lookup


def test_lookup_matches_h3(precipitation_df):
    lookup = precompute_h3_index_lookup(precipitation_df, 5)
    assert len(lookup) == 4
    for row in lookup.itertuples():
        assert row.h3_index == h3.latlng_to_cell(row.latitude, row.longitude, 5)


def test_enrich_computes_once_and_reuses_cache(precipitation_df, tmp_path, monkeypatch):
    df, lookup = enrich_with_h3(precipitation_df, 5, str(tmp_path))
    assert df["h3_index"].notna().all()
    assert len(df) == len(precipitation_df)
    assert os.path.exists(lookup_file_path(str(tmp_path), 5))

    def fail(*args, **kwargs):
        raise AssertionError("lookup should not be recomputed")

    monkeypatch.setattr(h3_processor, "precompute_h3_index_lookup", fail)
    enrich_with_h3(precipitation_df, 5, str(tmp_path), lookup)  # in memory
    df_from_disk, _ = enrich_with_h3(precipitation_df, 5, str(tmp_path))  # from disk, new run
    pd.testing.assert_frame_equal(df_from_disk, df)


def test_enrich_recomputes_when_grid_changes(precipitation_df, tmp_path):
    _, lookup = enrich_with_h3(precipitation_df, 5, str(tmp_path))
    bigger_grid = pd.concat([precipitation_df, precipitation_df.assign(latitude=48.0)], ignore_index=True)

    df, new_lookup = enrich_with_h3(bigger_grid, 5, str(tmp_path), lookup)

    assert df["h3_index"].notna().all()
    assert len(new_lookup) > len(lookup)
