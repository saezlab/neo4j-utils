from typing import Union
import re
import subprocess

from neo4j_utils._logger import logger

__all__ = ['Neo4jVersion']


class Neo4jVersion:
    """
    Provides version information for Neo4j.
    """

    major: Union[int, None]

    def __init__(self):
        """Get the neo4j version from the neo4j-admin command."""
        try:
            cmd = ['neo4j-admin', '--version']
            output = subprocess.check_output(cmd).decode().strip()
            version_match = re.search(r'(\d+\.\d+\.\d+)', output)
            if version_match:
                self.major = int(version_match.group(1).split('.')[0])
            else:
                logger.warning(
                    f'Unable to parse Neo4j version from command '
                    f'output: {output}',
                )
        except subprocess.CalledProcessError as e:
            logger.warning(f'Error running neo4j-admin: {e}')
        except (ValueError, IndexError) as e:
            logger.warning(f'Error detecting Neo4j version: {e}')

    @property
    def version(self):
        """Return the neo4j major version number."""
        return self.major
