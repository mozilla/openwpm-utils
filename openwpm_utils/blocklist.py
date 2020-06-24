from urllib.parse import urlparse
import domain_utils as du

import abp_blocklist_parser
import requests
import pyspark.sql.functions as F
from pyspark.sql.types import *


# Download blocklists
raw_lists = {
  'nocoin': 'https://raw.githubusercontent.com/hoshsadiq/adblock-nocoin-list/master/nocoin.txt',
  'ublock-resource-abuse': 'https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters/resource-abuse.txt'
}
# Initialize parsers
blockers = [
  abp_blocklist_parser.BlockListParser(regexes=requests.get(raw_lists["nocoin"]).content.decode()),
  abp_blocklist_parser.BlockListParser(regexes=requests.get(raw_lists["ublock-resource-abuse"]).content.decode())
]
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
"beacon":"ping",
"csp_report":"other",
"font":"font",
"image":"image",
"imageset":"image",
"main_frame":"document",
"media":"media",
"object":"object",
"object_subrequest":"object",
"ping":"ping",
"script":"script",
"speculative":"other",
"stylesheet":"stylesheet",
"sub_frame":"subdocument",
"web_manifest":"other",
"websocket":"websocket",
"xbl":"other",
"xml_dtd":"other",
"xmlhttprequest":"xmlhttprequest",
"xslt":"other",
"other":"other"
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
                "Argument %s given for `resource_type` not found in map." % resource_type
            )
    options["domain"] = urlparse(top_level_url).hostname
    
    # Add third-party option if third party. Value doesn't matter.
    if du.get_ps_plus_1(url) != du.get_ps_plus_1(top_level_url):
        options["third-party"] = True
    return options

def get_matching_rules(url, top_level_url, resource_type):
  # skip top-level requests
  if top_level_url is None:
    return
  
  matching_rules = set()
  options = get_option_dict(url, top_level_url, resource_type)
  
  for blocker in blockers:
    result = blocker.should_block_with_items(url, options=options)
    if result is not None and result[0] == 'blacklisted':
      matching_rules = matching_rules.union(result[1])
    if len(matching_rules) > 0:
      return tuple(matching_rules)
  return


udf_get_matching_rules = F.udf(get_matching_rules, ArrayType(StringType()))