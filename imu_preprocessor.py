import numpy as np


def get_row_wise_stat(col, data):
    col_stat = []
    division_factor = 1
    if col.startswith("ac"):
        division_factor = 16384
    if col.startswith("gc"):
        division_factor = 131

    for row in range(len(data)):
        row_stat = np.mean(data[col].iloc[row]) / division_factor
        col_stat.append(row_stat)
    return col_stat
