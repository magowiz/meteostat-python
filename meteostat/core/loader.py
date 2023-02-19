"""
Core Class - Data Loader

Meteorological data provided by Meteostat (https://dev.meteostat.net)
under the terms of the Creative Commons Attribution-NonCommercial
4.0 International Public License.

The code is licensed under the MIT license.
"""

from urllib.error import HTTPError
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from typing import Callable, Union
import pandas as pd
from meteostat.core.warn import warn
import io
import requests
import gzip
from io import StringIO
from kivy.logger import Logger
import shutil
import os
import errno




def processing_handler(
    datasets: list, load: Callable[[dict], None], cores: int, threads: int
) -> None:
    """
    Load multiple datasets (simultaneously)
    """

    # Data output
    output = []

    # Multi-core processing
    if cores > 1 and len(datasets) > 1:

        # Create process pool
        with Pool(cores) as pool:

            # Process datasets in pool
            output = pool.starmap(load, datasets)

            # Wait for Pool to finish
            pool.close()
            pool.join()

    # Multi-thread processing
    elif threads > 1 and len(datasets) > 1:

        # Create process pool
        with ThreadPool(threads) as pool:

            # Process datasets in pool
            output = pool.starmap(load, datasets)

            # Wait for Pool to finish
            pool.close()
            pool.join()

    # Single-thread processing
    else:

        for dataset in datasets:
            output.append(load(*dataset))

    # Remove empty DataFrames
    filtered = list(filter(lambda df: df.index.size > 0, output))

    return pd.concat(filtered) if len(filtered) > 0 else output[0]


def load_handler(
    endpoint: str,
    path: str,
    columns: list,
    types: Union[dict, None],
    parse_dates: list,
    coerce_dates: bool = False,
) -> pd.DataFrame:
    """
    Load a single CSV file into a DataFrame
    """

    try:
        # Read CSV file from Meteostat endpoint
        # endpoint = endpoint.replace('https', 'http')
        # Logger.info(f'meteostat endpoint {endpoint}')
        url = endpoint + path
        x = requests.get(url=url, verify=None).content
        gzipped = True
        file_out = 'file.txt'
        with gzip.open(io.BytesIO(x), 'rb') as fh:
            try:
                fh.read(1)
            except gzip.BadGzipFile:
                print('input_file is not a valid gzip file by BadGzipFile')
                gzipped = False
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), path)
        if gzipped:
            with gzip.open(io.BytesIO(x), 'rb') as f_in:
                with open(file_out, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        df = pd.read_csv(
            file_out,
            names=columns,
            dtype=types,
            parse_dates=parse_dates,
        )

        # Force datetime conversion
        if coerce_dates:
            df.iloc[:, parse_dates] = df.iloc[:, parse_dates].apply(
                pd.to_datetime, errors="coerce"
            )

    except (FileNotFoundError, HTTPError):

        # Create empty DataFrane
        df = pd.DataFrame(columns=[*types])

        # Display warning
        warn(f"Cannot load {path} from {endpoint}")

    # Return DataFrame
    return df
