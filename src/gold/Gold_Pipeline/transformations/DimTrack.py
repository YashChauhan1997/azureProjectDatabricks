import dlt

@dlt.table
def dimtrackStg():
    df=spark.readStream.table("spotify_cata.silver.dimtrack")
    return df

dlt.create_streaming_table(name="dimtrack")
dlt.create_auto_cdc_flow(
    target="dimtrack",
    source="dimtrackStg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=None,
    name=None,
    once=False
)