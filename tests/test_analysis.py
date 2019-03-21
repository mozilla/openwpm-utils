from openwpm_utils.analysis import (
    get_ordered_script_urls_from_call_stack,
    get_ordered_script_ps1s_from_call_stack
)

HTTPS_SCHEME = "https://"

STACK_JS_DOMAIN_1 = 'example-1.com'
STACK_JS_DOMAIN_2 = 'example-2.com'
STACK_JS_DOMAIN_3 = 'example-3.com'

STACK_JS_URL_1 = HTTPS_SCHEME + STACK_JS_DOMAIN_1
STACK_JS_URL_2 = HTTPS_SCHEME + STACK_JS_DOMAIN_2
STACK_JS_URL_3 = HTTPS_SCHEME + STACK_JS_DOMAIN_3

SAMPLE_STACK_TRACE_1 =\
    "func@" + STACK_JS_URL_1 + ":1:2;null\n"\
    "func@" + STACK_JS_URL_2 + ":3:4;null\n"\
    "func@" + STACK_JS_URL_3 + ":5:6;null"

SAMPLE_STACK_TRACE_2 =\
    "func@" + STACK_JS_URL_1 + ":1:2;null\n"\
    "func@" + STACK_JS_URL_2 + ":3:4;null\n"\
    "func@" + STACK_JS_URL_2 + ":3:4;null\n"\
    "func@" + STACK_JS_URL_2 + ":3:4;null\n"\
    "func@" + STACK_JS_URL_3 + ":5:6;null"


SAMPLE_STACK_TRACE_3 =\
    "func@" + STACK_JS_URL_1 + ":1:2;null\n"\
    "func@" + STACK_JS_URL_1 + ":1:2;null\n"\
    "func@" + STACK_JS_URL_3 + ":5:6;null\n"\
    "func@" + STACK_JS_URL_1 + ":1:2;null\n"\
    "func@" + STACK_JS_URL_2 + ":3:4;null\n"\
    "func@" + STACK_JS_URL_2 + ":3:4;null\n"\
    "func@" + STACK_JS_URL_2 + ":3:4;null\n"


EXPECTED_STACK_JS_URLS = ", ".join(
    [STACK_JS_URL_1, STACK_JS_URL_2, STACK_JS_URL_3])

EXPECTED_STACK_JS_PS1S = ", ".join(
    [STACK_JS_DOMAIN_1, STACK_JS_DOMAIN_2, STACK_JS_DOMAIN_3])

EXPECTED_STACK_JS_URLS_MIXED = ", ".join(
    [STACK_JS_URL_1, STACK_JS_URL_3,
     STACK_JS_URL_1, STACK_JS_URL_2])

EXPECTED_STACK_JS_PS1S_MIXED = ", ".join(
    [STACK_JS_DOMAIN_1, STACK_JS_DOMAIN_3,
     STACK_JS_DOMAIN_1, STACK_JS_DOMAIN_2])


def test_get_ordered_script_urls_from_call_stack():
    assert get_ordered_script_urls_from_call_stack(
        SAMPLE_STACK_TRACE_1) == EXPECTED_STACK_JS_URLS

    assert get_ordered_script_urls_from_call_stack(
        SAMPLE_STACK_TRACE_2) == EXPECTED_STACK_JS_URLS

    assert get_ordered_script_urls_from_call_stack(
        SAMPLE_STACK_TRACE_3) == EXPECTED_STACK_JS_URLS_MIXED


def test_get_ordered_script_ps1s_from_call_stack():
    assert get_ordered_script_ps1s_from_call_stack(
        SAMPLE_STACK_TRACE_1) == EXPECTED_STACK_JS_PS1S

    assert get_ordered_script_ps1s_from_call_stack(
        SAMPLE_STACK_TRACE_2) == EXPECTED_STACK_JS_PS1S

    assert get_ordered_script_ps1s_from_call_stack(
        SAMPLE_STACK_TRACE_3) == EXPECTED_STACK_JS_PS1S_MIXED
