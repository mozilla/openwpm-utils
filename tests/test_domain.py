from crawl_utils.domain import (
    get_ps_plus_1,
)


def test_get_ps_plus_one_cloudfront():
    assert get_ps_plus_1('https://my.domain.cloudfront.net') == 'domain.cloudfront.net'


def test_get_ps_plus_one_no_https():
    assert get_ps_plus_1('my.domain.cloudfront.net') == 'domain.cloudfront.net'
