import os
import glob
import logging
import argparse
import duckdb

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def merge_results(output_dir):
    """
    Merge all parquet files in output_dir and save as a single CSV.
    Uses DuckDB to process data efficiently without loading everything into memory.
    """
    logger.info(f"Starting merge process in {output_dir}")

    # List all parquet files
    files = glob.glob(os.path.join(output_dir, "part_*.parquet"))

    if not files:
        logger.warning("No parquet files found to merge.")
        return

    logger.info(f"Found {len(files)} parquet files. Processing with DuckDB...")

    con = None
    try:
        # Use DuckDB to read and group
        con = duckdb.connect(database=":memory:")

        # Register the parquet files as a view
        # We use a glob pattern to let DuckDB handle the file reading
        parquet_pattern = os.path.join(output_dir, "part_*.parquet")
        con.execute(
            f"CREATE OR REPLACE VIEW all_data AS SELECT * FROM read_parquet('{parquet_pattern}')"
        )

        # Output directory for merged files
        merged_dir = os.path.join(output_dir, "merged_results")
        os.makedirs(merged_dir, exist_ok=True)

        output_path = os.path.join(merged_dir, "all_enriched_data.csv")
        logger.info(f"Exporting all data to {output_path}")

        # Use DuckDB COPY to write directly to CSV
        query = f"""
            COPY (
                SELECT * FROM all_data 
            ) TO '{output_path}' (HEADER, DELIMITER ',')
        """
        con.execute(query)

        logger.info(f"Successfully merged and saved all data to {output_path}")

    except Exception as e:
        logger.error(f"Merge failed: {e}")
    finally:
        if con:
            try:
                con.close()
            except:
                pass


def main():
    parser = argparse.ArgumentParser(description="Merge Parquet files to CSV")
    parser.add_argument(
        "--output-dir", required=True, help="Directory containing parquet files"
    )
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        logger.error(f"Output directory {args.output_dir} does not exist.")
        return

    merge_results(args.output_dir)


if __name__ == "__main__":
    main()
