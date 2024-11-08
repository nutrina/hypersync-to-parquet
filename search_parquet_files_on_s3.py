import os
from eth_utils import to_bytes, to_checksum_address
import polars as pl
import boto3
from typing import List, Optional, Dict
from urllib.parse import urlparse
import concurrent.futures
from tqdm import tqdm
from botocore.config import Config


class S3PolarsSearcher:
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = "us-west-2",
        max_workers: int = 6,
    ):
        config = Config(retries=dict(max_attempts=20), max_pool_connections=50)

        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.max_workers = max_workers
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config,
        )

    def list_parquet_files(self, prefix: str = "") -> List[str]:
        """List all parquet files in the bucket with given prefix"""
        parquet_files = []
        paginator = self.s3_client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if "Contents" in page:
                    parquet_files.extend(
                        [
                            f"s3://{self.bucket_name}/{obj['Key']}"
                            for obj in page["Contents"]
                            if obj["Key"].endswith(".parquet")
                        ]
                    )

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
            print(f"Error getting schema: {e}")
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
                storage_options={
                    "aws_access_key_id": self.aws_access_key_id,
                    "aws_secret_access_key": self.aws_secret_access_key,
                },
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
    searcher = S3PolarsSearcher(
        bucket_name="arbitrum-erc20-transfer-events",
        aws_access_key_id="KEY",
        aws_secret_access_key="KEY",
        region_name="us-west-2",
        max_workers=50,
    )

    search_address = "0xDbf14bc7111e5F9Ed0423Ef8792258b7EBa8764c"

    # First check schema of one file
    first_file = searcher.list_parquet_files("erc20_transfers_arbitrum_")[0]
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
