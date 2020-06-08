from urllib.parse import urlparse

from .domain import get_ps_plus_1


def get_option_dict(url, top_level_url, content_type):
    """Build an options dict for BlockListParser

    Parameters
    ----------
    url : string
        The URL of the requested resource.
    top_level_url : string
        The URL of the top-level frame of the requested resource
    content_type : int
        An integer representing the content type of the load. Mapping given in:
        https://searchfox.org/mozilla-central/source/dom/base/nsIContentPolicy.idl

    Returns
    -------
    dict
        An "options" dictionary for use with BlockListParser
    """
    options = {}
    options["image"] = content_type == 3
    options["script"] = content_type == 2
    options["domain"] = urlparse(top_level_url).hostname
    options["third-party"] = get_ps_plus_1(url) != get_ps_plus_1(top_level_url)
    return options
