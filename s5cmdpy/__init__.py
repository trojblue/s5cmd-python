from typing import List, Tuple
from .s5cmd_runner import S5CmdRunner


def download_from_s3_list(s3_uris:List[str], dest_dir:str, simplified_print:bool=True):
    """
    Downloads a list of S3 URIs to a destination directory using s5cmd.
    
    :param s3_uris: List of S3 URIs to download
    :param dest_dir: Destination directory to download files to
    :param simplified_print: If True, prints simplified output every 5 seconds. If False, prints every line of output from s5cmd.
    """
    runner = S5CmdRunner()
    return runner.download_from_s3_list(s3_uris=s3_uris, dest_dir=dest_dir, simplified_print=simplified_print)


def mv(from_str:str, to_str:str):
    """
    Moves files from one location to another using s5cmd.
    Cannot move between two local directories.
    
    :param from_str: Source location to move files from
    :param to_str: Destination location to move files to
    """
    runner = S5CmdRunner()
    return runner.mv(from_str=from_str, to_str=to_str)


def cp(from_str:str, to_str:str, simplified_print=False, report_interval=10):
    """
    Copies files from one location to another using s5cmd.
    Cannot copy between two local directories.
    
    :param from_str: Source location to copy files from
    :param to_str: Destination location to copy files to
    """
    runner = S5CmdRunner()
    return runner.cp(from_str=from_str, to_str=to_str, simplified_print=simplified_print, report_interval=report_interval)


def run(txt_uri:str, simplified_print=True):
    """
    Run s5cmd with a command file specified by a local path, URL, or S3 URI.

    See test_s5cmdpy.ipynb for usage

    Args:
        txt_uri (str): The path, URL, or S3 URI of the command file.
        simplified_print (bool): Whether to use simplified progress display.
    """
    runner = S5CmdRunner()
    return runner.run(txt_uri=txt_uri, simplified_print=simplified_print)

    
def sync(source, destination, simplified_print=True, report_interval=10):
    """
    Syncs files from one location to another using s5cmd.
    
    :param source: Source location to sync files from
    :param destination: Destination location to sync files to
    """
    runner = S5CmdRunner()
    return runner.sync(source=source, destination=destination, simplified_print=simplified_print, report_interval=report_interval)


def ls(s3_uri, report_interval=5):
    """
    Lists files in an S3 URI using s5cmd.
    
    :param s3_uri: S3 URI to list files from
    """
    runner = S5CmdRunner()
    return runner.ls(s3_uri=s3_uri, report_interval=report_interval)