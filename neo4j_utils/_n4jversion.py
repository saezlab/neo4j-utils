import subprocess
import re

__all__ = ['Neo4jVersion']


class Neo4jVersion:
    '''
    Provides version information for Neo4j.
    '''

    major: int | None

    def __init__(self):
        try:
            cmd = ['neo4j-admin', '--version']
            output = subprocess.check_output(cmd).decode().strip()
            semver_pattern = r'\b\d+\.\d+\.\d+\b'
            matches = re.findall(semver_pattern, output)
            version = matches[0]
            # Output format: "X.Y.Z" or "X.Y.Z-SNAPSHOT"
            if version == None:
                raise ValueError(f"No neo4j version found in {output}")
            self.major = int(version.split('.')[0])
        except (subprocess.CalledProcessError, ValueError, IndexError) as e:
            print(f'Error detecting Neo4j version: {e}')
            self.major = None

    @property
    def version(self):
        return self.major
