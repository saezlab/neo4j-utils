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
A query focused interface.
"""

import collections

__all__ = ['Query']


class Query(
        collections.namedtuple('QueryBase', ('query', 'args')),
):
    """
    A Cypher query with arguments.
    """

    pass


Query.__new__.__defaults__ = (None,)
Query.__annotations__ = {'query': str, 'args': dict | None}
