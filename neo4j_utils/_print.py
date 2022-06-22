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
Pretty printing.
"""

from ._logger import logger

logger.debug(f'Loading module {__name__.strip("_")}.')

from typing import Optional

__all__ = ['bcolors', 'pretty', 'dict_str']


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def pretty(d, lines: Optional[list]=None, indent: int=0) -> list:
    """
    Pretty format a Neo4j profile dict.

    Takes Neo4j profile dictionary and an optional header as
    list and creates a list of strings to be printed.
    """

    lines = lines or []

    # if more items, branch
    if d:
        if isinstance(d, list):
            for sd in d:
                pretty(sd, lines, indent)
        elif isinstance(d, dict):
            typ = d.pop('operatorType', None)
            if typ:
                lines.append(
                    ('\t' * (indent))
                    + '|'
                    + '\t'
                    + f'{bcolors.OKBLUE}Step: {typ} {bcolors.ENDC}',
                )

            # buffer children
            chi = d.pop('children', None)

            for key, value in d.items():

                if key == 'args':

                    pretty(value, lines, indent)

                # both are there for some reason, sometimes
                # both in the same process
                elif key == 'Time' or key == 'time':

                    lines.append(
                        ('\t' * (indent))
                        + '|'
                        + '\t'
                        + str(key)
                        + ': '
                        + f'{bcolors.WARNING}{value:,}{bcolors.ENDC}'.replace(
                            ',', ' ',
                        ),
                    )

                else:

                    lines.append(
                        ('\t' * (indent))
                        + '|'
                        + '\t'
                        + str(key)
                        + ': '
                        + str(value),
                    )

            # now the children
            pretty(chi, lines, indent + 1)

    return lines


def dict_str(dct: dict) -> str:
    """
    String representation of a dict.
    """

    if not isinstance(dct, dict):

        return str(dct)

    return ', '.join(f'{str(key)}={str(val)}' for key, val in dct.items())
