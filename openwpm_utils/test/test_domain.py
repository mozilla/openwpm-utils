from __future__ import absolute_import

from ..domain import get_etld_plus_1
from .basetest import BaseTest


class TestDomainUtils(BaseTest):
    def test_etld_plus_1(self):
        assert get_etld_plus_1('http://www.google.com') == 'google.com'
        assert get_etld_plus_1('http://blah.bbc.co.uk') == 'bbc.co.uk'
        assert get_etld_plus_1('http://www.google.com') == 'google.com'
        assert get_etld_plus_1(
            'https://blah.example.com/foo/bar.html') == 'example.com'
        assert get_etld_plus_1(
            'http://foo.bar.website.apartments') == 'website.apartments'
        assert get_etld_plus_1(
            'http://foo.blah.apps.fbsbx.com') == 'blah.apps.fbsbx.com'
        assert get_etld_plus_1('http://foo.blah.www.fbsbx.com') == 'fbsbx.com'
        assert get_etld_plus_1('javascript:alert(1)') is None
        assert get_etld_plus_1(
            "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAA"
            "LAAAAAABAAEAAAIBRAA7") is None
        assert get_etld_plus_1('/foo.html') is None
        assert get_etld_plus_1('http://192.168.1.1') == '192.168.1.1'
        assert get_etld_plus_1('http://127.0.0.1/foo.html') == '127.0.0.1'
