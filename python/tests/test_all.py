import pytest
import phenolrs


def test_sum_as_string():
    assert phenolrs.sum_as_string(1, 1) == "2"
