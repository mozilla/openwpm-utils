from six.moves.urllib.parse import urlparse

from .domain import get_ps_plus_1


def get_option_dict(request):
    """Build an options dict for BlockListParser

    Parameters
    ----------
    request : sqlite3.Row
        A single HTTP request record pulled from OpenWPM's http_requests table
    public_suffix_list : PublicSuffixList
        An instance of PublicSuffixList()

    Returns
    -------
    dict
        An "options" dictionary for use with BlockListParser
    """
    options = {}
    options["image"] = request['content_policy_type'] == 3
    options["script"] = request['content_policy_type'] == 2
    options["domain"] = urlparse(request['top_level_url']).hostname
    options["third-party"] = get_ps_plus_1(
        request['url']) != get_ps_plus_1(request['top_level_url'])
    return options
