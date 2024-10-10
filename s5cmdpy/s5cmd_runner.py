import os
import subprocess
import platform
import requests
import re
from tqdm.auto import tqdm
from urllib.parse import urlparse
import time
import shutil
import hashlib

from s5cmdpy.uni_logger_standalone import UniLogger

from typing import List

class S5CmdRunner:
    """
    A class that provides methods for interacting with s5cmd, a command-line tool for efficient S3 data transfer.

    Attributes:
        s5cmd_path (str): The path to the s5cmd executable.

    Methods:
        __init__(): Initializes the S5CmdRunner object.
        has_s5cmd() -> bool: Checks if s5cmd is available.
        get_s5cmd() -> None: Downloads and installs s5cmd if it is not available.
        call_function(command: str, *args): Calls a function with the specified command and arguments.
        download_file(file_uri) -> str: Downloads a file from a URI to a temporary local path.
        generate_s5cmd_file(s3_uris, dest_dir) -> str: Generates a command file for s5cmd with the specified S3 URIs and destination directory.
        download_from_s3_list(s3_uris, dest_dir): Downloads multiple files from S3 using s5cmd.
        is_local_file(path) -> bool: Checks if a file path is a local file.
        download_from_url(url) -> str: Downloads a file from a URL to a temporary local path.
        cp(from_str, to_str): Copies a file from a local path or URL to an S3 URI or vice versa using s5cmd.
        mv(from_str, to_str): Moves a file from a local path to an S3 URI or vice versa using s5cmd.
        run(txt_uri): Runs s5cmd with a command file specified by a local path, URL, or S3 URI.
    """

    def __init__(self):
        # if on windows
        binary_name = 's5cmd' if os.name != 'nt' else 's5cmd.exe'
        self.s5cmd_path = os.path.expanduser(f'~/{binary_name}')
        
        self.logger = UniLogger()
        if not self.has_s5cmd():
            self.get_s5cmd()

    def has_s5cmd(self) -> bool:
        return os.path.exists(self.s5cmd_path) and os.access(self.s5cmd_path, os.X_OK)

    def get_s5cmd(self) -> None:
        arch = platform.machine()
        s5cmd_url = ""

        if arch == 'x86_64':
            s5cmd_url = "https://huggingface.co/kiriyamaX/s5cmd-backup/resolve/main/s5cmd_2.2.2_Linux-64bit/s5cmd"
        elif arch == 'aarch64':
            s5cmd_url = "https://huggingface.co/kiriyamaX/s5cmd-backup/resolve/main/s5cmd_2.2.2_Linux-arm64/s5cmd"
        
        # windows support
        elif arch == 'AMD64':
            s5cmd_url = "https://huggingface.co/kiriyamaX/s5cmd-backup/resolve/main/s5cmd_2.2.2_Windows-amd64/s5cmd.exe"
        else:
            raise ValueError("Unsupported architecture")

        try:
            response = requests.get(s5cmd_url)
            response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
            with open(self.s5cmd_path, 'wb') as file:
                file.write(response.content)
            # Set executable permissions on Unix-like systems
            if os.name != 'nt':
                os.chmod(self.s5cmd_path, 0o755)
            self.logger.info("s5cmd downloaded and installed successfully.")
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download s5cmd: {e}")

    def call_function(self, command: str, *args, capture_output=False):
        """
        return a process when capture_output is True, otherwise execute and return None.
        """
        # Ensure s5cmd is available before proceeding
        if not self.has_s5cmd():
            self.logger.warning("s5cmd is not available, attempting to download and install.")
            self.get_s5cmd()
            # Recheck if s5cmd is now available, raise error if not
            if not self.has_s5cmd():
                self.logger.error("Failed to ensure s5cmd is available.")
                raise RuntimeError("Failed to ensure s5cmd is available.")
        
        if capture_output:
            try:
                process = subprocess.Popen(
                    [command, *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                return process
            except Exception as e:
                self.logger.error(f"Error starting subprocess: {e}")
                return None
        else:
            subprocess.run([command, *args])

    @staticmethod
    def fast_list_hash(s3_uris: List[str]) -> str:
        """Generate a hash for a list of S3 URIs.
        """
        if not s3_uris:
            raise ValueError("The s3_uris list cannot be empty.")

        # Generate a simplified identifier based on the URIs list characteristics
        first_uri = s3_uris[0]
        last_uri = s3_uris[-1]
        middle_uri = s3_uris[len(s3_uris) // 2]
        total_length = sum(len(uri) for uri in s3_uris)
        num_uris = len(s3_uris)
        identifier = hashlib.md5(f"{num_uris}-{len(first_uri)}-{len(last_uri)}-{len(middle_uri)}-{total_length}".encode()).hexdigest()
        return identifier[:8]


    def generate_s5cmd_file(self, s3_uris: List[str], dest_dir:str):        
        if not s3_uris:
            raise ValueError("The s3_uris list cannot be empty.")

        # Generate a simplified identifier based on the URIs list characteristics
        identifier = self.fast_list_hash(s3_uris)
        date_str = time.strftime("%Y%m%d-%H%M%S")
        command_file_path = f'/tmp/s5cmd_commands_{date_str}_{identifier}.txt'

        with open(command_file_path, 'w') as file:
            for s3_uri in s3_uris:
                command = f"cp {s3_uri} {dest_dir}/{os.path.basename(s3_uri)}\n"
                file.write(command)
        
        return command_file_path


    def _update_progress_bar(self, process, total=None, report_interval=5):
        """
        Updates a progress bar based on subprocess output.
        
        Args:
            process (subprocess.Popen): The subprocess.Popen object.
            total (int, optional): The total number of items expected to process. If None, progress bar is indeterminate.
            report_interval (int): Frequency in seconds to update the progress report.
        """
        start_time = last_report_time = time.time()
        line_count = 0
        with tqdm(total=total, desc=f"[interval={report_interval}] running s5cmd") as pbar:
            for line in process.stdout:
                line_count += 1
                current_time = time.time()
                # Update progress bar if report_interval has passed or process completes
                if current_time - last_report_time >= report_interval or process.poll() is not None:
                    pbar.update(line_count)
                    line_count = 0
                    last_report_time = current_time
            # Final update to ensure progress bar reflects total work done
            pbar.update(line_count)
        process.wait()
        # Handle case where process ends before the first report interval
        if time.time() - start_time < report_interval:
            pbar.n = total or pbar.n
            pbar.refresh()


    def download_from_s3_list(self, s3_uris:List[str], dest_dir:str, simplified_print:bool=True):

        command_file_path = self.generate_s5cmd_file(s3_uris, dest_dir)

        if simplified_print:
            process = self.call_function(
                self.s5cmd_path, "run", command_file_path, capture_output=True)
            if process and process.stdout:
                self._update_progress_bar(process, total=len(s3_uris))
            else:
                self.logger.error("Failed to start s5cmd subprocess with output capture.")
        else:
            self.logger.info(f"Generated command file: {command_file_path}")

            self.logger.info(f"Downloading {len(s3_uris)} files from S3 to {dest_dir}")
            self.call_function(self.s5cmd_path, "run", command_file_path)
            self.logger.info(f"Downloaded {len(s3_uris)} files from S3 to {dest_dir}")

            print(f"removing command file: {command_file_path}")

        # remove the command file after use
        try:
            os.remove(command_file_path)
        except Exception as e:
            self.logger.error(f"Failed to remove command file: {e}")

    def is_local_file(self, path:str):
        return os.path.isfile(path)

    def get_filename_from_url(self, url):
        """
        Extract the filename from a URL.

        Args:
            url (str): The URL to parse.

        Returns:
            str: The filename extracted from the URL.
        """
        parsed_url = urlparse(url)
        return os.path.basename(parsed_url.path)

    def download_file(self, uri:str):
        """
        Download a file from a URI (S3 or HTTP/HTTPS URL) to a temporary local path, preserving the filename.
        Args:
            uri (str): The URI of the file to download, can be an S3 URI or a URL.
        Returns:
            str: The local path of the downloaded file.
        """
        if uri.startswith('s3://'):
            local_filename = self.get_filename_from_url(uri)  # Use the S3 key as the filename
            local_path = os.path.join('/tmp', local_filename)
            self.call_function(self.s5cmd_path, "cp", uri, local_path)
        elif re.match(r'https?://', uri):
            local_filename = self.get_filename_from_url(uri)
            local_path = os.path.join('/tmp', local_filename)
            response = requests.get(uri)
            response.raise_for_status()
            with open(local_path, 'wb') as file:
                file.write(response.content)
        else:
            raise ValueError("Unsupported URI scheme")
        return local_path

    def mv(self, from_str:str, to_str:str):
        """
        Move a file from a local path to an S3 URI or from an S3 URI to a local path.

        Args:
            from_str (str): The source file path or S3 URI.
            to_str (str): The destination file path or S3 URI.
        """
        self.call_function(self.s5cmd_path, "mv", from_str, to_str)

    def cp(self, from_str, to_str, simplified_print=False, report_interval=10):

        downloaded_path = None
        if re.match(r'https?://', from_str):
            # Download file for HTTP/HTTPS URIs and temporarily save the local path
            downloaded_path = self.download_file(from_str)
            from_str = downloaded_path

        # Warning for uploading a file name instead of into a folder
        if os.path.isfile(from_str) and not to_str.endswith('/'):
            file_extension = os.path.splitext(from_str)[1]
            if file_extension:
                print(f"Warning: '{from_str}' is being uploaded as a file name '{to_str}' instead of into a folder.")

        if downloaded_path:
            # If file was downloaded, decide on the next action based on destination
            if to_str.startswith('s3://'):
                # If destination is S3, use s5cmd to upload
                self.call_function(self.s5cmd_path, "cp", downloaded_path, to_str)
            else:
                # If destination is local, use shutil.move
                shutil.move(downloaded_path, to_str)
        elif from_str.startswith('s3://') and not to_str.startswith('s3://'):
            # Use s5cmd to copy from S3 to local path directly
            self.call_function(self.s5cmd_path, "cp", from_str, to_str)
        else:
            # For all other cases, including S3 to S3, use call_function directly
            self.call_function(self.s5cmd_path, "cp", from_str, to_str)

    def run(self, txt_uri, simplified_print=True):
        """
        Run s5cmd with a command file specified by a local path, URL, or S3 URI.

        See test_s5cmdpy.ipynb for usage

        Args:
            txt_uri (str): The path, URL, or S3 URI of the command file.
            simplified_print (bool): Whether to use simplified progress display.
        """

        if not self.is_local_file(txt_uri):
            txt_uri = self.download_file(txt_uri)

        process = self.call_function(self.s5cmd_path, "run", txt_uri, capture_output=simplified_print)
        if simplified_print and process and process.stdout:
            # Assuming we don't parse txt_uri to count commands, we leave total=None for an indeterminate progress bar
            self._update_progress_bar(process)
        elif not simplified_print:
            # The call_function already handled the subprocess.run case
            pass
        else:
            self.logger.error("Failed to start s5cmd subprocess with output capture.")

    def sync(self, source, destination, simplified_print=True, report_interval=10):
        """
        Sync a folder to another folder using s5cmd.

        Args:
            source (str): The source path.
            destination (str): The destination path.
            simplified_print (bool): Whether to use simplified progress display.
            report_interval (int): Frequency in seconds to update the progress report.
        """

        # Adjust source path for local folder without trailing slash
        if not source.startswith('s3://') and os.path.isdir(source) and not source.endswith('/'):
            self.logger.warning("Local source path does not end with a slash. Matching s5cmd behavior with `aws s3 cp`.")
            source += '/'
            self.logger.warning(f"Adjusted source path: {source}")

        # Adjust source path for S3 without pattern
        if source.startswith('s3://') and not source.endswith('/*'):
            self.logger.warning("S3 source path does not end with a pattern.")
            source = source.rstrip("/") + '/*'
            self.logger.warning(f"Adjusted source path: {source}")

        # Adjust destination path for S3 without trailing slash
        if destination.startswith('s3://') and not destination.endswith('/'):
            self.logger.warning("S3 destination path does not end with a slash.")
            destination += '/'
            self.logger.warning(f"Adjusted destination path: {destination}")

        process = self.call_function(self.s5cmd_path, "sync", source, destination, capture_output=simplified_print)
        if simplified_print and process and process.stdout:
            # For sync, total could potentially be determined by listing source files ahead of time for a more accurate progress bar
            self._update_progress_bar(process, report_interval=report_interval)
        elif not simplified_print:
            # The call_function already handled the subprocess.run case
            pass
        else:
            self.logger.error("Failed to start s5cmd subprocess with output capture.")

    def ls(self, s3_uri, report_interval=5):
        """
        Lists objects in an S3 bucket and returns details in a structured format.

        Args:
            s3_uri (str): The S3 URI to list objects from.
            report_interval (int): Frequency in seconds to update the progress report.

        Returns:
            dict: A dictionary where keys are the paths of the files, and values are tuples containing size and date.
        """
        process = self.call_function(self.s5cmd_path, "ls", s3_uri, capture_output=True)
        if not process:
            self.logger.error("Failed to start s5cmd subprocess with output capture for listing.")
            return {}

        output_dict = {}
        line_count = 0
        start_time = last_report_time = time.time()

        with tqdm(desc="Listing S3 objects") as pbar:
            for line in process.stdout:
                match = re.search(r'^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)$', line.strip())
                if match:
                    date, size, path = match.groups()
                    output_dict[path] = (int(size), date)
                    line_count += 1
                    current_time = time.time()
                    if current_time - last_report_time >= report_interval or process.poll() is not None:
                        pbar.update(line_count)
                        line_count = 0
                        last_report_time = current_time

            # Final update to ensure progress bar reflects total work done
            pbar.update(line_count)

        process.wait()
        # Handle case where process ends before the first report interval
        if time.time() - start_time < report_interval:
            pbar.n = pbar.n
            pbar.refresh()

        return output_dict

if __name__ == '__main__':
    # Example usage:
    runner = S5CmdRunner()
    runner.run('s3://your-bucket/path-to-your-file.txt')
    # Or
    runner.run('http://example.com/path-to-your-file.txt')
