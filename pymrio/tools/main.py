import logging
import os
import re
import warnings
import zipfile
from collections import namedtuple
from pandas.api.types import is_any_real_numeric_dtype

import numpy as np
import pandas as pd

# Constants and global variables
from pymrio.core.constants import PYMRIO_PATH
from pymrio.core.fileio import load_all
from pymrio.core.mriosystem import Extension, IOSystem
from pymrio.tools.iometadata import MRIOMetaData
from pymrio.tools.ioutil import get_repo_content, sniff_csv_format
from ioparser import parse_new_oecd, IDX_NAMES
from pathlib import Path

path = Path('/Users/andreasst/work/raw_v2023')
year = 1995
#oecd = parse_new_oecd(path, 2000)

path = os.path.abspath(os.path.normpath(str(path)))


# determine which oecd file to be parsed
if not os.path.isdir(path):
    # 1. case - one file specified in path
    oecd_file = path
    path = os.path.split(oecd_file)[0]
else:
    # 2. case: dir given - build oecd_file with the value given in year
    if not year:
        raise ParserError(
            "No year specified "
            "(either specify a specific file "
            "or path and year)"
        )

    oecd_file_list = [
        fl
        for fl in os.listdir(path)
        if (
            os.path.splitext(fl)[-1] in [".csv", ".CSV", ".zip"]
            and os.path.splitext(fl)[0]
            in [str(year)+oo for oo in [".SML", ""]]
        )
    ]

    if len(oecd_file_list) > 1:
        unique_file_data = set([os.path.splitext(fl)[0] for fl in oecd_file_list])

        if len(unique_file_data) > 1:
            raise ParserError(
                "Multiple files for a given year "
                "found (specify a specific file in the "
                'parameter "path")'
            )

    elif len(oecd_file_list) == 0:
        raise FileNotFoundError("No data file for the given year found")

    oecd_file = os.path.join(path, oecd_file_list[0])

oecd_file_name = os.path.split(oecd_file)[1]

try:
    years = re.findall(r"\d\d\d\d", oecd_file_name)
    oecd_version = "v" + years[0]
    oecd_year = years[1]
    meta_desc = "OECD ICIO for {}".format(oecd_year)

except IndexError:
    oecd_version = "n/a"
    oecd_year = "n/a"
    meta_desc = "OECD ICIO - year undefined"

meta_rec = MRIOMetaData(
    location=path,
    name="OECD-ICIO",
    description=meta_desc,
    version=oecd_version,
    system="IxI",  # base don the readme
)

oecd_raw = pd.read_csv(oecd_file, sep=",", index_col=0).fillna(0)
meta_rec._add_fileio("OECD data parsed from {}".format(oecd_file))

mon_unit = "Million USD"

oecd_totals_col = ["TOTAL"]
oecd_totals_row = ["OUT", "OUTPUT"]

oecd_raw.drop(oecd_totals_col, axis=1, errors="ignore", inplace=True)
oecd_raw.drop(oecd_totals_row, axis=0, errors="ignore", inplace=True)

# Important - these must not match any country or industry name
factor_input = oecd_raw.filter(regex="VALU|TAX", axis=0)
final_demand = oecd_raw.filter(
    regex="HFCE|NPISH|NPS|GGFC|GFCF|INVNT|INV|DIRP|DPABR|FD|P33|DISC", axis=1
)

Z = oecd_raw.loc[
    oecd_raw.index.difference(factor_input.index),
    oecd_raw.columns.difference(final_demand.columns),
]
F_factor_input = factor_input.loc[
    :, factor_input.columns.difference(final_demand.columns)
]
F_Y_factor_input = factor_input.loc[:, final_demand.columns]
Y = final_demand.loc[final_demand.index.difference(F_factor_input.index), :]

Z_index = pd.MultiIndex.from_tuples(tuple(ll) for ll in Z.index.str.split("_"))
Z_columns = Z_index.copy()
Z_index.names = IDX_NAMES["Z_row"]