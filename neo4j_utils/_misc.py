#!/usr/bin/env python

#
# Copyright 2021-2022, Heidelberg University Hospital
#
# File author(s): Denes Turei <turei.denes@gmail.com>
#                 Sebastian Lobentanzer
#
# Distributed under the MIT (Expat) license, see the file `LICENSE`.
#

"""
Small, general purpose snippets used in various parts of the module.
"""

from typing import Any, Mapping, KeysView, Generator, ItemsView, ValuesView

__all__ = ['LIST_LIKE', 'if_none', 'to_list']

LIST_LIKE = (
    list,
    set,
    tuple,
    Generator,
    ItemsView,
    KeysView,
    Mapping,
    ValuesView,
)


def if_none(*values) -> Any:
    """
    Use the first item in from ``values`` that is not ``None``.
    """

    for v in values:

        if v is not None:

            return v


def to_list(value: Any) -> list:
    """
    Ensures that ``value`` is a list.
    """

    if isinstance(value, LIST_LIKE):

        value = list(value)

    elif value is None:

        value = []

    else:

        value = [value]

    return value


def to_tuple(value: Any) -> tuple:
    """
    Ensures that ``value`` is a tuple.
    """

    return tuple(to_list(value))
