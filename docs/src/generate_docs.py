from distutils.log import error
import json
import pathlib

ERROR_COUNTERS_JSON_PATH = pathlib.Path(__file__).parent / "error_counters.json"
ERROR_COUNTERS_DOCS_PATH = pathlib.Path(__file__).parent / "../error_messages.md"

if __name__ == "__main__":
    output_md = """
# Error Counters
Issues found in the input files are categorized under "counters". Counters
aggregate the issues, and provide a high-level overview of what went wrong.

This page is auto-generated from `error_counters.json` to provide an 
overview of all error counters and suggested actions to remedy the issues.

Error counters with **Is Prefix?: Yes** are generated with a suffix that gives
some details on the subject of the error. For example,
`Resolution_UnresolvedExternalId_` will be appended with the ID that is not 
resolved in the reports generated by `dc-import`.
    """
    with open(ERROR_COUNTERS_JSON_PATH, "r") as input_json:
        error_counters = json.load(input_json)["error_counters"]

    for counter in error_counters:
        counter_name = counter["counter_name"]
        counter_is_prefix = counter["is_prefix"]
        counter_level = counter["level"]
        counter_user_actions = counter["user_actions"]
        
        # f-string expression part cannot include a backslash, so we use nl
        nl = "\n" 
        output_md += f"""
## {counter_name}
**Issue level**: {counter_level.split("_")[1].capitalize()}

**Is Prefix?**: {"Yes" if counter_is_prefix else "No"}

{"### Suggested User Actions" if counter_user_actions else ""}
{nl.join(["1. " + user_action for user_action in counter_user_actions])}
        """

    with open(ERROR_COUNTERS_DOCS_PATH, "w+") as output_markdown:
        output_markdown.write(output_md)