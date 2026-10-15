import dlt

expectations = {
    "rule_1": "user_id IS NOT NULL"
}

# 1. تعريف جدول الـ Staging وتطبيق الـ Expectations عليه
@dlt.table(
    comment="Staging table for user dimension with quality expectations"
)
@dlt.expect_all_or_drop(expectations)
def DimUser_stg():
    df = spark.readStream.table("spotify_cata.silver.dimuser")
    return df

# 2. إنشاء الجدول المستهدف (Target Table) مرة واحدة فقط باسم "dimuser"
dlt.create_streaming_table(
    name="dimuser",
    comment="Target dimension table for users with SCD Type 2"
)

# 3. تطبيق تدفق الـ CDC لنقل البيانات من الـ Staging إلى الـ Target
dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)