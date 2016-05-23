#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import hmac
import hashlib
import base64
import config
from urllib import quote_plus


def hash_hmac(key, data):
    """md5和sha1已经很容易被破解，采用hmac+sha256"""
    return base64.b64encode(
        hmac.new(key, data, hashlib.sha256).digest())


def str_to_sign(method, url, args):
    """合成需要签名的字符串

    Args:
        method: HTTP 请求方法，纯大写
        url: 去掉 query_string 后的URL
        args: (dict) body 或者 query_string 中的参数和值

    """
    if 'sign' in args:
        del(args['sign'])

    arr = []
    for key in sorted(args):
        arr.append('%s=%s' % (key, args[key]))
    ordered_query_string = '&'.join(arr)


    raw = '&'.join([method, quote_plus(url), quote_plus(ordered_query_string)])
    return raw


