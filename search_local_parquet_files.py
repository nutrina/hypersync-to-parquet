import os
from eth_utils import to_bytes, to_checksum_address
import polars as pl
from typing import List, Optional, Dict
import concurrent.futures
from tqdm import tqdm


class LocalParquetSearcher:
    def __init__(
        self,
        directory_path: str,
        max_workers: int = 6,
    ):
        self.directory_path = os.path.abspath(directory_path)
        self.max_workers = max_workers

    def list_parquet_files(self, prefix: str = "") -> List[str]:
        """List all parquet files in the directory with given prefix"""
        parquet_files = []

        try:
            for root, _, files in os.walk(self.directory_path):
                for file in files:
                    if file.endswith(".parquet") and (
                        not prefix or file.startswith(prefix)
                    ):
                        full_path = os.path.join(root, file)
                        parquet_files.append(full_path)

            print(f"Found {len(parquet_files)} parquet files")
            return parquet_files
        except Exception as e:
            print(f"Error listing files: {e}")
            return []

    def get_schema(self, file_path: str) -> pl.Schema:
        """Get schema of a parquet file"""
        try:
            return pl.scan_parquet(file_path).schema
        except Exception as e:
            print(f"Error getting schema for {file_path}: {e}")
            return None

    def process_file_chunk(self, files: List[str], address: str) -> pl.DataFrame:
        """Process a chunk of parquet files"""
        try:
            schema = self.get_schema(files[0])
            if not schema:
                return pl.DataFrame()

            columns = [
                "from_address",
                "to_address",
                "value",
                "block_number",
                "transaction_hash",
            ]

            available_columns = [col for col in columns if col in schema.keys()]

            df = pl.scan_parquet(
                files,
                cache=True,
                rechunk=True,
                low_memory=True,
            )

            # Apply filter
            query = df.filter(
                (pl.col("from_address") == address) | (pl.col("to_address") == address)
            )

            return query.collect(streaming=True)

        except Exception as e:
            print(f"Error processing chunk {files[:2]}: {e}")
            if isinstance(e, pl.ColumnNotFoundError):
                print(f"Available columns: {schema.keys() if schema else 'unknown'}")
            return pl.DataFrame()

    def search_transactions(
        self,
        search_address: str,
        file_prefix: str = "",
        batch_size: int = 50,
        max_files: Optional[int] = None,
    ) -> pl.DataFrame:
        """Search for transactions involving the specified address"""
        parquet_files = self.list_parquet_files(file_prefix)

        if max_files:
            parquet_files = parquet_files[:max_files]

        if not parquet_files:
            print(f"No parquet files found with prefix: {file_prefix}")
            return pl.DataFrame()

        # Split files into chunks
        file_chunks = [
            parquet_files[i : i + batch_size]
            for i in range(0, len(parquet_files), batch_size)
        ]

        results_list = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            futures = {
                executor.submit(self.process_file_chunk, chunk, search_address): i
                for i, chunk in enumerate(file_chunks)
            }

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(file_chunks),
                desc="Processing files",
            ):
                try:
                    result = future.result()
                    if not result.is_empty():
                        results_list.append(result)
                except Exception as e:
                    chunk_idx = futures[future]
                    print(f"Error processing chunk {chunk_idx}: {e}")

        if not results_list:
            return pl.DataFrame()

        # Combine results efficiently
        results = pl.concat(results_list, rechunk=True)

        return results


def main():
    searcher = LocalParquetSearcher(
        directory_path="./oxdbf_test",
        max_workers=50,
    )

    search_address = "0xDbf14bc7111e5F9Ed0423Ef8792258b7EBa8764c"

    # First check schema of one file
    first_file = searcher.list_parquet_files("erc20_transfers_")[0]
    print("\nParquet Schema:")
    print(searcher.get_schema(first_file))

    results = searcher.search_transactions(
        search_address=search_address,
        file_prefix="erc20_transfers_arbitrum_",
        batch_size=10,
        max_files=None,
    )

    print(f"\nFound {len(results)} transactions")
    if not results.is_empty():
        print("\nSample transactions:")
        print(results.head())

        # Calculate total value if the column exists
        if "value" in results.columns:
            total_value = results.select(pl.col("value").sum()).item()
            print(f"\nTotal value: {total_value}")

        # Save results efficiently
        results.write_parquet(
            "search_results.parquet", compression="snappy", statistics=False
        )


if __name__ == "__main__":
    main()
