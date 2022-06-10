from distutils.log import error
import json
import pathlib

_ERROR_COUNTERS_JSON_PATH = pathlib.Path(__file__).parent / "error_counters.json"
_ERROR_COUNTERS_DOCS_PATH = pathlib.Path(__file__).parent / "../error_messages.md"

_DOCUMENTATION_HEAD = """# Counters

Issues found in the input files are categorized under "counters" in `report.json`
and `summary_report.html`. Counters aggregate the issues, and provide a
high-level overview of what went wrong.

Counters with "**Suffix Description:**" are prefix counters that are
logged with a suffix detailing the subject of the issue. For example,
`Resolution_UnresolvedExternalId_` will be suffixed with the ID that could not 
be resolved. The "**Suffix Description:**" field describes the nature of the
suffix for these counters.

"""

_MARKDOWN_LINEBREAK = "\n\n"

def generate_counter_documentation(counter):
    """
    Given a counter from error_counters.json, returns markdown documentation
    as a string.

    The returned string will have no leading or trailing newlines.
    """

    counter_name = counter["counter_name"]
    counter_user_actions = counter["user_actions"]

    # .get() returns None if key does not exist
    counter_description = counter.get("description")
    counter_suffix_description = counter.get("suffix_description")
    
    name_str = f"## {counter_name}"

    description_str = ""
    if counter_description:
        description_str = f"**Description:** {counter_description}"

    suffix_description_str = ""
    if counter_suffix_description:
        suffix_description_str = f"**Suffix Description:** {counter_description}"
    
    user_actions_str = ""
    if counter_user_actions:
        user_actions_str = "### Suggested User Actions\n\n"
        user_actions_str += "\n".join(["1. " + user_action for user_action in counter_user_actions])

    documentation_parts = [
        name_str,
        description_str,
        suffix_description_str,
        user_actions_str
    ]
    
    # filter out empty parts
    documentation_parts = [part for part in documentation_parts if part]

    counter_documentation = _MARKDOWN_LINEBREAK.join(documentation_parts)

    return counter_documentation

def generate_documentation(input_json_path, output_md_path):
    """
    Given `input_json_path` with the path where error_counters.json is stored
    and the desired output path `output_md_path`, generates markdown 
    documentation and writes it to the output path.
    """
    
    with open(input_json_path, "r") as input_json:
        error_counters = json.load(input_json)["error_counters"]

    generated_counters_documentation = _MARKDOWN_LINEBREAK.join([
        generate_counter_documentation(counter)
        for counter in error_counters
    ])
    
    output_md = _DOCUMENTATION_HEAD
    output_md += generated_counters_documentation
    output_md += "\n" # end the file with a trailing newline

    with open(output_md_path, "w+") as output_markdown:
        output_markdown.write(output_md)

if __name__ == "__main__":
    generate_documentation(_ERROR_COUNTERS_JSON_PATH, _ERROR_COUNTERS_DOCS_PATH)