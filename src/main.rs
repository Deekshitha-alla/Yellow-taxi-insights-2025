use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::Result;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat; 
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::prelude::*;
use std::sync::Arc;

fn print_heading(title: &str) {
    println!("\n{}", "=".repeat(90));
    println!("{}", title);
    println!("{}", "=".repeat(90));
}

async fn show_dataframe(title: &str, df: DataFrame) -> Result<()> {
    let batches = df.collect().await?;
    print_heading(title);
    let formatted = pretty_format_batches(&batches)?;
    println!("{formatted}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let parquet_format = ParquetFormat::default();
    let listing_options = ListingOptions::new(Arc::new(parquet_format));

    ctx.register_listing_table(
        "yellow_trips_2025",
        "D:/NYC",
        listing_options,
        None,
        None,
    )
    .await?;

    println!("Loaded parquet files from D:/NYC");

    // -----------------------------
    // DataFrame API - Aggregation 1
    // -----------------------------
    let df_base_1 = ctx
        .sql(
            r#"
            SELECT
                EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,
                total_amount,
                fare_amount
            FROM yellow_trips_2025
            "#,
        )
        .await?;

    let df_agg_1 = df_base_1
        .aggregate(
            vec![col("pickup_month")],
            vec![
                count(col("pickup_month")).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("average_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, true)])?;

    show_dataframe(
        "DataFrame API - Aggregation 1: Trips and revenue by month",
        df_agg_1,
    )
    .await?;

    // -----------------------------
    // DataFrame API - Aggregation 2
    // -----------------------------
    let df_base_2 = ctx
        .sql(
            r#"
            SELECT
                payment_type,
                tip_amount,
                total_amount
            FROM yellow_trips_2025
            "#,
        )
        .await?;

    let df_grouped_2 = df_base_2.aggregate(
        vec![col("payment_type")],
        vec![
            count(col("payment_type")).alias("trip_count"),
            avg(col("tip_amount")).alias("average_tip_amount"),
            sum(col("tip_amount")).alias("sum_tip_amount"),
            sum(col("total_amount")).alias("sum_total_amount"),
        ],
    )?;

    let df_agg_2 = df_grouped_2
        .select(vec![
            col("payment_type"),
            col("trip_count"),
            col("average_tip_amount"),
            (col("sum_tip_amount") / col("sum_total_amount")).alias("tip_rate"),
        ])?
        .sort(vec![col("trip_count").sort(false, false)])?;

    show_dataframe(
        "DataFrame API - Aggregation 2: Tip behavior by payment type",
        df_agg_2,
    )
    .await?;

    // -----------------------------
    // SQL - Aggregation 1
    // -----------------------------
    let sql_agg_1 = ctx
        .sql(
            r#"
            SELECT
                EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(fare_amount) AS average_fare
            FROM yellow_trips_2025
            GROUP BY 1
            ORDER BY 1 ASC
            "#,
        )
        .await?;

    show_dataframe(
        "SQL - Aggregation 1: Trips and revenue by month",
        sql_agg_1,
    )
    .await?;

    // -----------------------------
    // SQL - Aggregation 2
    // -----------------------------
    let sql_agg_2 = ctx
        .sql(
            r#"
            SELECT
                payment_type,
                COUNT(*) AS trip_count,
                AVG(tip_amount) AS average_tip_amount,
                SUM(tip_amount) / SUM(total_amount) AS tip_rate
            FROM yellow_trips_2025
            GROUP BY payment_type
            ORDER BY trip_count DESC
            "#,
        )
        .await?;

    show_dataframe(
        "SQL - Aggregation 2: Tip behavior by payment type",
        sql_agg_2,
    )
    .await?;

    println!("\nAll aggregations completed successfully.");
    println!("The tables or summaries printed.");

    Ok(())
} 