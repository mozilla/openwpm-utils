from typing import Any, List, Optional, Set, Tuple
from urllib.parse import urlparse

import domain_utils as du
import pyspark.sql.functions as F
import requests
from abp_blocklist_parser import BlockListParser
from pyspark.sql.types import ArrayType, StringType

# Mapping from
# https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/API/webRequest/ResourceType
# To
# https://help.eyeo.com/en/adblockplus/how-to-write-filters#options
# With help from
# https://github.com/gorhill/uBlock/blob/1d5800629aaca0d152127d844ca7f6cf975f2f68/src/js/static-net-filtering.js#L55
# https://github.com/gorhill/uBlock/blob/1d5800629aaca0d152127d844ca7f6cf975f2f68/src/js/static-net-filtering.js#L107
# Import both objects and use
# let directMapping = {}
# Object.entries(typeNameToTypeValue).forEach(k => {directMapping[k[0]]= typeValueToTypeName[k[1] >>4]})
# to generate an object that represents the direct mapping between these types

type_to_option = {
    "beacon": "ping",
    "csp_report": "other",
    "font": "font",
    "image": "image",
    "imageset": "image",
    "main_frame": "document",
    "media": "media",
    "object": "object",
    "object_subrequest": "object",
    "ping": "ping",
    "script": "script",
    "speculative": "other",
    "stylesheet": "stylesheet",
    "sub_frame": "subdocument",
    "web_manifest": "other",
    "websocket": "websocket",
    "xbl": "other",
    "xml_dtd": "other",
    "xmlhttprequest": "xmlhttprequest",
    "xslt": "other",
    "other": "other",
}


def get_option_dict(url, top_level_url, resource_type=None):
    """Build an options dict for BlockListParser.

    These options are checked here:
    * https://github.com/englehardt/abp-blocklist-parser/blob/40f6bb5b91ea403b7b9852a16d6c57d5ec26cf7f/abp_blocklist_parser/RegexParser.py#L104-L117
    * https://github.com/englehardt/abp-blocklist-parser/blob/40f6bb5b91ea403b7b9852a16d6c57d5ec26cf7f/abp_blocklist_parser/RegexParser.py#L240-L248

    Parameters
    ----------
    url : string
        The URL of the requested resource.
    top_level_url : string
        The URL of the top-level frame of the requested resource
    resource_type : string
        All possible values are here https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/API/webRequest/ResourceType

    Returns
    -------
    dict
        An "options" dictionary for use with BlockListParser
    """
    options = {}
    # Add type option. Value doesn't matter.
    if resource_type:
        try:
            options[type_to_option[resource_type]] = True
        except KeyError:
            raise ValueError(
                "Argument %s given for `resource_type` not found in map."
                % resource_type
            )
    options["domain"] = urlparse(top_level_url).hostname
    if options["domain"] == None:
        # If somehow the top_level_url should be unparseable
        return None

    # Add third-party option if third party. Value doesn't matter.
    if du.get_ps_plus_1(url) != du.get_ps_plus_1(top_level_url):
        options["third-party"] = True
    return options


def prepare_get_matching_rules(blockers: List[BlockListParser]) -> Any:
    def get_matching_rules(
        url: str, top_level_url: str, resource_type: Optional[str]
    ) -> Optional[Tuple[Any, ...]]:
        # skip top-level requests
        if top_level_url is None:
            return None

        matching_rules: Set[str] = set()
        options = get_option_dict(url, top_level_url, resource_type)
        if options is None:
            print(
                f"Something went wrong when handling {url} on top level URL {top_level_url}"
            )
            return None

        for blocker in blockers:
            result = blocker.should_block_with_items(url, options=options)
            if result is not None and result[0] == "blacklisted":
                matching_rules = matching_rules.union(result[1])

        if len(matching_rules) > 0:
            return tuple(matching_rules)
        return None

    return F.udf(get_matching_rules, ArrayType(StringType()))
