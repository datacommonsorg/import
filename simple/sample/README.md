# Import Samples

This folder contains sample input files under the `input` folder and the generated output files under the `output` folder.

To run import on the sample input files, `cd` into the `simple` folder and then run:

```bash
python3 -m stats.main --input_dir=sample/input --output_dir=sample/output
```

If you are planning to checkin the sample outputs to github, 
consider running with the `--freeze_time` flag.
This freezes the reporting time to `2023-01-01` and avoids updates to `report.json` on every run.

```bash
python3 -m stats.main --input_dir=sample/input --output_dir=sample/output --freeze_time
```

