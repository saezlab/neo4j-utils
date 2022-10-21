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
Configuration of the module logger.
"""

__all__ = ['get_logger', 'log', 'logfile', 'log_traceback']

from datetime import datetime
import os
import sys
import pydoc
import logging
import tempfile
import itertools
import traceback

import colorlog

from ._metadata import __version__


def get_logger(name: str = 'neo4ju') -> logging.Logger:
    """
    Access the module logger, create a new one if does not exist yet.

    The file handler creates a log file named after the current date and
    time. Levels to output to file and console can be set here.

    Args:
        name:
            Name of the logger instance.

    Returns:
        An instance of the Python :py:mod:`logging.Logger`.
    """

    if not logging.getLogger(name).hasHandlers():

        file_formatter = logging.Formatter(
            '[ %(asctime)s ] [ %(levelname)s ] [ %(module)s ] %(message)s',
        )

        now = datetime.now()
        date_time = now.strftime('%Y%m%d-%H%M%S')

        logdir = os.path.join(tempfile.gettempdir(), 'neo4j-utils-log')
        os.makedirs(logdir, exist_ok=True)
        logfile = os.path.join(logdir, f'neo4j-utils-{date_time}.log')

        file_handler = logging.FileHandler(logfile)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)

        stdout_handler = colorlog.StreamHandler()
        stdout_handler.setLevel(logging.WARN)
        stdout_handler.setFormatter(
            colorlog.ColoredFormatter(
                fmt = (
                    '%(log_color)s'
                    '[%(asctime)s.%(msecs)-3d] '
                    '%(func_in_brackets)s '
                    '%(level_in_brackets)s '
                    '%(message)s'
                ),
                datefmt = '%Y-%m-%d %H:%M:%S',
            ),
        )

        _logfactory = logging.getLogRecordFactory()


        def _logfactory_patched(*args, **kwargs):

            record = _logfactory(*args, **kwargs)
            record.func_in_brackets = f'[{record.name}.{record.funcName}]'
            record.func_in_brackets = '%-30s' % record.func_in_brackets
            record.level_in_brackets = '[%s]' % record.levelname
            record.level_in_brackets = '%-10s' % record.level_in_brackets

            return record


        logging.setLogRecordFactory(_logfactory_patched)

        logger = logging.getLogger(name)
        logger.addHandler(file_handler)
        logger.addHandler(stdout_handler)
        logger.setLevel(logging.DEBUG)

        logger.info(f'This is Neo4j utils v{__version__}.')
        logger.info(f'Using Python {sys.version}.')
        logger.info(f'Logging into `{logfile}`.')

    return logging.getLogger(name)


def logfile() -> str:
    """
    Path to the log file.
    """

    return get_logger().handlers[0].baseFilename


def log():
    """
    Browse the log file.
    """

    with open(logfile()) as fp:

        pydoc.pager(fp.read())


def log_traceback():
    """
    Includes the last traceback into the log.
    """

    exc_type, exc_value, exc_traceback = sys.exc_info()

    if exc_type is not None:

        f = exc_traceback.tb_frame.f_back
        stack = traceback.extract_stack(f)

    else:

        stack = traceback.extract_stack()[:-1]

    trc_head = 'Traceback (most recent call last):\n'
    trc_lines = list(
        itertools.chain(
            *(
                stack_level.strip('\n').split('\n')
                for stack_level in traceback.format_list(stack)
            )
        ),
    )

    if exc_type is not None:

        trc_lines.extend(
            ('  %s' % traceback.format_exc().lstrip(trc_head)).split('\n'),
        )

    stack_top = 0

    for i, line in enumerate(trc_lines):

        if line.strip().endswith('<module>'):

            stack_top = i

    trc_lines = trc_lines[stack_top:]

    logger.error(trc_head.strip())

    for traceline in trc_lines:

        logger.error(traceline)


logger = get_logger()
