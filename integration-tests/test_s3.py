# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import io
import os
import subprocess

import smart_open

_S3_URL = os.environ.get('SO_S3_URL')
assert _S3_URL is not None, 'please set the SO_S3_URL environment variable'


def initialize_bucket():
    subprocess.check_call(['aws', 's3', 'rm', '--recursive', _S3_URL])


def write_read(key, content, write_mode, read_mode, encoding=None, **kwargs):
    with smart_open.smart_open(key, write_mode, encoding=encoding, **kwargs) as fout:
        fout.write(content)
    kwargs.pop('s3_upload', None)    
    with smart_open.smart_open(key, read_mode, encoding=encoding, **kwargs) as fin:
        actual = fin.read()
    return actual


def test_s3_readwrite_text(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt'
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, key, text, 'w', 'r', 'utf-8')
    assert actual == text


def test_s3_readwrite_text_gzip(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt.gz'
    text = 'не чайки здесь запели на знакомом языке'
    actual = benchmark(write_read, key, text, 'w', 'r', 'utf-8')
    assert actual == text


def test_s3_readwrite_binary(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt'
    binary = b'this is a test'
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary


def test_s3_readwrite_binary_gzip(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt.gz'
    binary = b'this is a test'
    actual = benchmark(write_read, key, binary, 'wb', 'rb')
    assert actual == binary

def test_s3_readwrite_zip(benchmark):
    def write_read_zip(key, binary):
        with smart_open.smart_open(key, "wb", ignore_extension=True) as fout:
            fout.write(binary)
        with smart_open.smart_open(key, "rb", member="cp852.tsv.txt", encoding="cp852") as fin:
            actual_text1 = fin.read()
        with smart_open.smart_open(key, "rb", member="crime-and-punishment.txt", encoding="utf-8") as fin:
            actual_text2 = fin.read()
        return actual_text1, actual_text2

    initialize_bucket()

    key = _S3_URL + '/sanity.zip'
    with open("smart_open/tests/test_data/two_files.zip", "rb") as in_file:
        binary = in_file.read()
    with open("smart_open/tests/test_data/cp852.tsv.txt", "r", encoding='cp852') as in_file:
        text1 = in_file.read()
    with open("smart_open/tests/test_data/crime-and-punishment.txt", "r") as in_file:
        text2 = in_file.read()
    actual_text1, actual_text2 = benchmark(write_read_zip, key, binary)
    assert actual_text1 == text1 and actual_text2 == text2

def test_s3_performance(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _S3_URL + '/performance.txt'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte


def test_s3_performance_gz(benchmark):
    initialize_bucket()

    one_megabyte = io.BytesIO()
    for _ in range(1024*128):
        one_megabyte.write(b'01234567')
    one_megabyte = one_megabyte.getvalue()

    key = _S3_URL + '/performance.txt.gz'
    actual = benchmark(write_read, key, one_megabyte, 'wb', 'rb')
    assert actual == one_megabyte

def test_s3_encrypted_file(benchmark):
    initialize_bucket()

    key = _S3_URL + '/sanity.txt'
    text = 'с гранатою в кармане, с чекою в руке'
    actual = benchmark(write_read, key, text, 'w', 'r', 'utf-8', s3_upload={
        'ServerSideEncryption': 'AES256'
    })
    assert actual == text


