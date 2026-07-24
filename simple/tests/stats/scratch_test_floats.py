import numpy as np
import pandas as pd


def convert_old(col):
  is_int_value = col.notna() & (col == col.round())
  is_na = col.isna()
  return np.where(
      is_int_value,
      col.round().astype("Int64").astype(str),
      np.where(is_na, "", col.astype(str)),
  )


def convert_new_str(col):
  # Convert to string and remove trailing .0
  s = col.astype(str)
  # Only replace .0 if it's at the end of the string
  return s.str.replace(r'\.0$', '', regex=True)


test_vals = [
    100.0, 100.5, 100000000000000.0, 100000000000000.0001, 0.0000000000001,
    np.nan
]

df = pd.DataFrame({"val": test_vals})
print("INPUTS:")
print(df["val"])
print("\nOLD CONVERSION:")
print(convert_old(df["val"]))
print("\nNEW STR CONVERSION:")
print(convert_new_str(df["val"]))
