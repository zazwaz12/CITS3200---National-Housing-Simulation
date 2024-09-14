"""
String utilities for working with placeholders in strings
"""

import re
from functools import reduce

from ..logging import log_entry_exit


@log_entry_exit()
def capture_placeholders(
    s: str, placeholders: list[str], re_pattern: str = r".*?"
) -> str:
    """
    Replace placeholders in a string `s` with `"(re_pattern)"`

    Placeholders are strings in the form of ``"{placeholder}"``. They can be any
    combination of numbers, characters, and underscore. Placeholders in `s`
    but not in the list of `placeholders` will not be encased in parentheses
    (i.e., not in a capturing group) but will still be replaced.

    Parameters
    ----------
    s : str
        String containing placeholders, e.g. ``"somestuff{a}_{b}.nii.gz"``.
    placeholders : list[str]
        List of placeholders to match in the pattern, e.g. ``["a", "b"]``.
    re_pattern : str, optional
        Regex pattern to replace placeholders with, by default any valid Python
        identifier. Matches any character except line terminators by default.

    Returns
    -------
    str
        String with placeholders replaced by the specified `re_pattern`.
    """
    # Replace placeholders to avoid them being escaped
    x = reduce(
        lambda string, placeholder: string.replace(f"{{{placeholder}}}", "\x00"),
        [s] + placeholders,
    )
    # Replace all non-capturing placeholders with a different symbol
    x = re.sub(r"{[a-zA-Z0-9_]*}", "\x01", x)
    x = re.escape(x)
    # Encase provided placeholders in parentheses to create capturing groups
    x = x.replace("\x00", f"({re_pattern})")
    x = x.replace("\x01", re_pattern)
    return str(x)


@log_entry_exit()
def placeholder_matches(
    str_list: list[str],
    pattern: str,
    placeholders: list[str],
    re_pattern: str = r".*?",
) -> list[tuple[str, ...]]:
    """
    Return placeholder values for each string in a list that match pattern.

    Placeholders are string in the form of "{placeholder}". They can be any
    combination of numbers, characters, and underscore. Placeholders not in
    pattern will still be matched but not captured in the output.

    Parameters
    ----------
    str_list: list[str]
        List of strings to match against the pattern.
    pattern: str
        Pattern containing placeholders to match file names, e.g.
        `"/path/to/{organ}_{observer}.nii.gz"`.
    placeholders: list[str]
        List of placeholders to match in the pattern, e.g. `["organ", "observer"]`.
    re_pattern: str, optional
        Regex pattern to filter placeholder matches by, by default any character
        except line terminators.

    Returns
    -------
    list[tuple[str, ...]]
        List of tuples containing the matches for each placeholder.

    Example
    -------
    ```
    placeholder_matches(
        [
            "/path/to/bladder_jd.nii.gz",
            "/path/to/brain_md.nii.gz",
            "/path/to/eye_sp.nii.gz",
        ],
        "/path/to/{organ}_{observer}.nii.gz",
        ["organ", "observer"],
    )
    # Output
    [("bladder", "jd"), ("brain", "md"), ("eye", "sp")]

        placeholder_matches(
        [
            "/path/to/bladder_jd.nii.gz",
            "/path/to/brain_md.nii.gz",
            "/path/to/eye_sp.nii.gz",
        ],
        "/path/to/{organ}_{observer}.nii.gz",
        ["organ", "observer"],
        r"[^b]+",    # Any string that does not contain the letter 'b'
    )
    # Output
    [("eye", "sp")]

    ```
    """

    def _capture(string: str) -> re.Match[str] | None:
        # Purely using named function because python type checking sucks
        return re.match(capture_placeholders(pattern, placeholders, re_pattern), string)

    x = map(_capture, str_list)
    x = filter(lambda match: match is not None, x)
    x = map(lambda re_match: re_match.groups() if re_match else (), x)
    return list(x)
