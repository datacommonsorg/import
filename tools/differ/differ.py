# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from absl import app
from absl import flags
import pandas as pd
import re
from google.cloud.storage import Client

FLAGS = flags.FLAGS
flags.DEFINE_string("currentDataFile", "", "Path to the current data file.")
flags.DEFINE_string("previousDataFile", "", "Path to the previous data file.")
flags.DEFINE_string("outputFile", "diff.csv", "Path to the diff output file.")
flags.DEFINE_string("fileType", "local", "local/gcs")
flags.DEFINE_string("fileFormat", "mcf", "mcf/csv")
# Required for CSV format files.
flags.DEFINE_string("groupbyColumns", "observationAbout,variableMeasured", "Columns to group data for diff analysis.")
flags.DEFINE_string("targetColumns", "value", "Column names with target variables for diff analysis.")

"""
This utility generates a diff of two data files of the same dataset for analysis.

Usage:
$ python differ.py --currentDataFile=<filepath> --previousDataFile=<filepath>

It supports reading either a local file or from a GCS bucket which can be selected using flag fileType.
File format supported are CSV and MCF which can be selected using flag fileFormat.

Output generated is of the form below showing counts of differences.
Detailed output is written to a file for further analysis.

variableMeasured  deleted  added    modified
0  dcid:var1      1.0        NaN        NaN
1  dcid:var2      NaN        1.0        NaN
2  dcid:var3      NaN        NaN        1.0

"""

class DataDiffer:
    def __init__(self):
        self.groupby_columns = FLAGS.groupbyColumns.split(',')
        self.target_columns = FLAGS.targetColumns.split(',')

    # Performs diff analysis by performing an outer join on the two datasets.
    def diff_analysis(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        res  = pd.merge(df1, df2, on = self.groupby_columns, how='outer', indicator='exist')
        res = res.loc[(res['exist'] != 'both') | (res['value_x'] != res['value_y'])]
        result = res[['variableMeasured','exist']]
        res = res.sort_values(by=['exist'])
        res.to_csv(FLAGS.outputFile, index=False)
        result = result.groupby(['variableMeasured','exist'], observed=True, as_index=False).size()
        result = result.pivot(index='variableMeasured', columns='exist', values='size').reset_index().rename_axis(None, axis=1)
        result.rename(columns={'left_only':'deleted', 'right_only':'added', 'both': 'modified'}, inplace=True)
        return result

    # Reads an MCF text file as a dataframe.
    def load_mcf_file(self, file: str) -> pd.DataFrame:
        mcf_contents = open(file, 'r').read()
        mcf_nodes_text = mcf_contents.split("\n\n") # nodes separated by a blank line
        mcf_line = re.compile(r"^(\w+): (.*)$") # lines seprated as property: constraint
        mcf_nodes = []
        for node in mcf_nodes_text:
            current_mcf_node = {}
            for line in node.split('\n'):
                parsed_line = mcf_line.match(line)
                if parsed_line is not None:
                    current_mcf_node[parsed_line.group(1)] = parsed_line.group(2)
            if current_mcf_node:
                mcf_nodes.append(current_mcf_node)
        df = pd.DataFrame(mcf_nodes)
        return df 

    # Downloads a GCS file to local.
    def get_gcs_file(self, uri: str):
        client = Client()
        bucket = client.get_bucket(uri.split('/')[2])
        fileName = uri.split('/')[3]
        blob = bucket.get_blob(fileName)
        blob.download_to_filename(fileName)
        return fileName

    # Loads a CSV dataset into a dataframe.
    def load_csv_file(self, file: str) -> pd.DataFrame:
        df = pd.read_csv(file)
        df = df[self.groupby_columns + self.target_columns]
        # Transform the dataset to convert each target variable into a separate obseration.
        df = pd.melt(df, id_vars=self.groupby_columns, var_name='variableMeasured', value_name='value')
        self.groupby_columns.append('variableMeasured')
        return df

def main(_):
    """Runs the code."""
    differ = DataDiffer()
    if FLAGS.fileType == "gcs":
        currentDataFile = differ.get_gcs_file(FLAGS.currentDataFile)
        previousDataFile = differ.get_gcs_file(FLAGS.previousDataFile)
    else: # local
        currentDataFile = FLAGS.currentDataFile
        previousDataFile = FLAGS.previousDataFile

    if FLAGS.fileFormat == "csv":
        df1 = differ.load_csv_file(previousDataFile)
        df2 = differ.load_csv_file(currentDataFile)
    else: #mcf
        df1 = differ.load_mcf_file(previousDataFile)
        df2 = differ.load_mcf_file(currentDataFile)

    result = differ.diff_analysis(df1, df2)
    print(result.head())

if __name__ == "__main__":
    app.run(main)
