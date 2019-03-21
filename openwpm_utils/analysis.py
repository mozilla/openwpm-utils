import json
from datetime import datetime

from pandas import read_sql_query

from .domain import get_ps_plus_1


def get_set_of_script_hosts_from_call_stack(call_stack):
    """Return the urls of the scripts involved in the call stack."""
    script_urls = set()
    if not call_stack:
        return ""
    stack_frames = call_stack.strip().split("\n")
    for stack_frame in stack_frames:
        script_url = stack_frame.rsplit(":", 2)[0].\
            split("@")[-1].split(" line")[0]
        script_urls.add(get_host_from_url(script_url))
    return ", ".join(script_urls)


def get_host_from_url(url):
    return strip_scheme_www_and_query(url).split("/", 1)[0]


def strip_scheme_www_and_query(url):
    """Remove the scheme and query section of a URL."""
    if url:
        return url.split("//")[-1].split("?")[0].lstrip("www.")
    else:
        return ""


def get_initiator_from_call_stack(call_stack):
    """Return the bottom element of the call stack."""
    if call_stack and type(call_stack) == str:
        return call_stack.strip().split("\n")[-1]
    else:
        return ""


def get_initiator_from_req_call_stack(req_call_stack):
    """Return the bottom element of a request call stack.
    Request call stacks have an extra field (async_cause) at the end.
    """
    return get_initiator_from_call_stack(req_call_stack).split(";")[0]


def get_func_and_script_url_from_initiator(initiator):
    """Remove line number and column number from the initiator."""
    if initiator:
        return initiator.rsplit(":", 2)[0].split(" line")[0]
    else:
        return ""


def get_script_url_from_initiator(initiator):
    """Remove the scheme and query section of a URL."""
    if initiator:
        return initiator.rsplit(":", 2)[0].split("@")[-1].split(" line")[0]
    else:
        return ""


def get_set_of_script_urls_from_call_stack(call_stack):
    """Return the urls of the scripts involved in the call stack as a
    string."""
    if not call_stack:
        return ""
    return ", ".join(get_script_urls_from_call_stack_as_set(call_stack))


def get_script_urls_from_call_stack_as_set(call_stack):
    """Return the urls of the scripts involved in the call stack as a set."""
    script_urls = set()
    if not call_stack:
        return script_urls
    stack_frames = call_stack.strip().split("\n")
    for stack_frame in stack_frames:
        script_url = stack_frame.rsplit(":", 2)[0].\
            split("@")[-1].split(" line")[0]
        script_urls.add(script_url)
    return script_urls


def get_ordered_script_urls_from_call_stack(call_stack):
    """Return the urls of the scripts involved in the call stack as a
    string. Preserve order in which the scripts appear in the call stack."""
    if not call_stack:
        return ""
    return ", ".join(get_script_urls_from_call_stack_as_list(
        call_stack))


def get_script_urls_from_call_stack_as_list(call_stack):
    """Return the urls of the scripts involved in the call stack as a list."""
    script_urls = []
    if not call_stack:
        return script_urls
    stack_frames = call_stack.strip().split("\n")
    last_script_url = ""
    for stack_frame in stack_frames:
        script_url = stack_frame.rsplit(":", 2)[0].\
            split("@")[-1].split(" line")[0]

        if script_url != last_script_url:
            script_urls.append(script_url)
            last_script_url = script_url
    return script_urls


def get_set_of_script_ps1s_from_call_stack(script_urls):
    if len(script_urls):
        return ", ".join(
            set((get_ps_plus_1(x) or "") for x in script_urls.split(", ")))
    else:
        return ""


def get_ordered_script_ps1s_from_call_stack(call_stack):
    """Return ordered list of script PS1s as they appear in the call stack."""
    return get_ordered_script_ps1s_from_stack_script_urls(
        get_ordered_script_urls_from_call_stack(call_stack))


def get_ordered_script_ps1s_from_stack_script_urls(script_urls):
    """Return ordered script PS1s as a string given a list of script URLs."""
    script_ps1s = []
    last_ps1 = None
    for script_url in script_urls.split(", "):
        ps1 = get_ps_plus_1(script_url) or ""
        if ps1 != last_ps1:
            script_ps1s.append(ps1)
            last_ps1 = ps1

    return ", ".join(script_ps1s)


def add_col_bare_script_url(js_df):
    """Add a col for script URL without scheme, www and query."""
    js_df['bare_script_url'] =\
        js_df['script_url'].map(strip_scheme_www_and_query)


def add_col_set_of_script_urls_from_call_stack(js_df):
    js_df['stack_scripts'] =\
        js_df['call_stack'].map(get_set_of_script_urls_from_call_stack)


def add_col_unix_timestamp(df):
    df['unix_time_stamp'] = df['time_stamp'].map(datetime_from_iso)


def datetime_from_iso(iso_date):
    """Convert from ISO."""
    iso_date = iso_date.rstrip("Z")
    # due to a bug we stored timestamps
    # without a leading zero, which can
    # be interpreted differently
    # .21Z should be .021Z
    # dateutils parse .21Z as .210Z

    # add the missing leading zero
    if iso_date[-3] == ".":
        rest, ms = iso_date.split(".")
        iso_date = rest + ".0" + ms
    elif iso_date[-2] == ".":
        rest, ms = iso_date.split(".")
        iso_date = rest + ".00" + ms

    try:
        # print(iso_date)
        return datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        # print "ISO", iso_date,
        # datetime.strptime(iso_date.rstrip("+0000"), "%Y-%m-%dT%H:%M:%S")
        return datetime.strptime(iso_date.rstrip("+0000"), "%Y-%m-%dT%H:%M:%S")


def get_cookie(headers):
    # not tested
    for item in headers:
        if item[0] == "Cookie":
            return item[1]
    return ""


def get_set_cookie(header):
    # not tested
    for item in json.loads(header):
        if item[0] == "Set-Cookie":
            return item[1]
    return ""


def get_responses_from_visits(con, visit_ids):
    visit_ids_str = "(%s)" % ",".join(str(x) for x in visit_ids)
    qry = """SELECT r.id, r.crawl_id, r.visit_id, r.url,
                sv.site_url, sv.first_party, sv.site_rank,
                r.method, r.referrer, r.headers, r.response_status, r.location,
                r.time_stamp FROM http_responses as r
            LEFT JOIN site_visits as sv
            ON r.visit_id = sv.visit_id
            WHERE r.visit_id in %s;""" % visit_ids_str

    return read_sql_query(qry, con)


def get_requests_from_visits(con, visit_ids):
    visit_ids_str = "(%s)" % ",".join(str(x) for x in visit_ids)
    qry = """SELECT r.id, r.crawl_id, r.visit_id, r.url, r.top_level_url,
            sv.site_url, sv.first_party, sv.site_rank,
            r.method, r.referrer, r.headers, r.loading_href, r.req_call_stack,
            r.content_policy_type, r.post_body, r.time_stamp
            FROM http_requests as r
            LEFT JOIN site_visits as sv
            ON r.visit_id = sv.visit_id
            WHERE r.visit_id in %s;""" % visit_ids_str

    return read_sql_query(qry, con)


def add_col_set_of_script_ps1s_from_call_stack(js_df):
    js_df['stack_script_ps1s'] =\
        js_df['stack_scripts'].map(get_set_of_script_ps1s_from_call_stack)


if __name__ == '__main__':
    pass
