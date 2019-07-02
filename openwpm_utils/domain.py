from __future__ import absolute_import, print_function

from functools import wraps
from ipaddress import ip_address

import six
import tldextract
from six.moves import range
from six.moves.urllib.parse import urlparse


def load_and_update_extractor(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        if 'extractor' not in kwargs:
            if wrapper.extractor is None:
                extract_tld = tldextract.TLDExtract(
                    include_psl_private_domains=True
                )
                extract_tld.update()
                wrapper.extractor = extract_tld
            return function(*args, extractor=wrapper.extractor, **kwargs)
        else:
            return function(*args, **kwargs)
    wrapper.extractor = None
    return wrapper


@load_and_update_extractor
def get_etld_plus_1(url, prepend_scheme=True, **kwargs):
    """Returns the eTLD+1 (aka PS+1) of the url. This will also return
    an IP address if the hostname of the url is a valid
    IP address.

    Parameters
    ----------
    prepend_scheme : boolean, optional
        If a scheme is not found in the url, prepend `http://` to avoid errors
    kwargs
        An (optional) tldextract::TLDExtract instance can be passed with
        keyword `extractor`, otherwise we create and update one automatically.
    """
    if 'extractor' not in kwargs:
        raise ValueError(
            "A tldextract::TLDExtract instance must be passed using the "
            "`extractor` keyword argument.")
    hostname = urlparse(url).hostname
    if is_ip_address(hostname):
        return hostname
    if hostname is None:
        # Possible reasons hostname is None, `url` is:
        # * malformed
        # * a relative url
        # * a `javascript:` or `data:` url
        # * many others
        return
    parsed = kwargs['extractor'](hostname)
    if parsed.domain == '':
        return parsed.suffix
    if parsed.suffix == '':
        return
    return "%s.%s" % (parsed.domain, parsed.suffix)


def get_ps_plus_1(url, **kwargs):
    """Returns the eTLD+1 (aka PS+1) of the url. This will also return
    an IP address if the hostname of the url is a valid
    IP address. Alias for `get_etld_plus_1`"""
    return get_etld_plus_1(url, **kwargs)


def is_ip_address(hostname):
    """
    Check if the given string is a valid IP address
    """
    try:
        ip_address(six.text_type(hostname))
        return True
    except ValueError:
        return False


@load_and_update_extractor
def hostname_subparts(url, include_ps=False, **kwargs):
    """
    Returns a list of slices of a url's hostname down to the PS+1

    If `include_ps` is set, the hostname slices will include the public suffix

    For example: http://a.b.c.d.com/path?query#frag would yield:
        [a.b.c.d.com, b.c.d.com, c.d.com, d.com] if include_ps == False
        [a.b.c.d.com, b.c.d.com, c.d.com, d.com, com] if include_ps == True

    An (optional) tldextract::TLDExtract instance can be passed with keyword
    arg `extractor`, otherwise we create and update one automatically.
    """
    if 'extractor' not in kwargs:
        raise ValueError(
            "A tldextract::TLDExtract instance must be passed using the "
            "`extractor` keyword argument.")
    hostname = urlparse(url).hostname

    # If an IP address, just return a single item list with the IP
    if is_ip_address(hostname):
        return [hostname]

    subparts = list()
    parsed = kwargs['extractor'](hostname)
    if parsed.domain == '':
        ps_plus_1 = parsed.suffix
    else:
        ps_plus_1 = "%s.%s" % (parsed.domain, parsed.suffix)

    # We expect all ps_plus_1s to have at least one '.'
    # If they don't, the url was likely malformed, so we'll just return an
    # empty list
    if '.' not in ps_plus_1:
        return []
    subdomains = hostname[:-(len(ps_plus_1) + 1)].split('.')
    if subdomains == ['']:
        subdomains = []
    for i in range(len(subdomains)):
        subparts.append('.'.join(subdomains[i:]) + '.' + ps_plus_1)
    subparts.append(ps_plus_1)
    if include_ps:
        try:
            subparts.append(ps_plus_1[ps_plus_1.index('.') + 1:])
        except Exception:
            pass
    return subparts


def get_stripped_url(url, scheme=False):
    """Returns a url stripped to (scheme)?+hostname+path"""
    purl = urlparse(url)
    surl = ''
    if scheme:
        surl += purl.scheme + '://'
    try:
        surl += purl.hostname + purl.path
    except TypeError:
        surl += purl.hostname
    return surl


def get_stripped_urls(urls, scheme=False):
    """ Returns a set (or list) of urls stripped to (scheme)?+hostname+path """
    new_urls = list()
    for url in urls:
        get_stripped_url(url, scheme)
    if type(urls) == set:
        return set(new_urls)
    return new_urls
