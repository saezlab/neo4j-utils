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

from typing import Any

__all__ = ['if_none']


def if_none(*values) -> Any:
    """
    Use the first item in from ``values`` that is not ``None``.
    """

    for v in values:

        if v is not None:

            return v
