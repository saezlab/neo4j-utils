import subprocess

class Neo4j_Version_Detection:

    '''
    Provides version information for Neo4j.

    '''

    major: int | None

    def __init__(self):
        try:
            cmd = ["neo4j-admin", "--version"]
            version = subprocess.check_output(cmd).decode().strip()
            # Output format: "X.Y.Z" or "X.Y.Z-SNAPSHOT"
            self.major = int(version.split(".")[0])
        except (subprocess.CalledProcessError, ValueError, IndexError) as e:
            print(f"Error detecting Neo4j version: {e}")
            self.major = None

    def get_version(self):
        return self.major
