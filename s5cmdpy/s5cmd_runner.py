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
        # Optional defaults for s5cmd configuration
        self.default_endpoint_url = None
        self.default_credentials_file = None
        self.default_region = None  # e.g., "apac" for Cloudflare R2
        self.ignore_aws_profile = True  # avoid inheriting local AWS profile by default
        if not self.has_s5cmd():
            self.get_s5cmd()

    def configure(self, endpoint_url: str = None, credentials_file: str = None, region: str = None, ignore_aws_profile: bool = None) -> None:
        """Configure default s5cmd options.

        Args:
            endpoint_url (str, optional): Custom S3-compatible endpoint URL.
            credentials_file (str, optional): Path to s5cmd credentials file.
            region (str, optional): Region for the S3-compatible endpoint (e.g., Cloudflare R2: apac, wnam, weur, etc.).
            ignore_aws_profile (bool, optional): If True, unset AWS_PROFILE/AWS_CONFIG_FILE in subprocess env.
        """
        if endpoint_url is not None:
            self.default_endpoint_url = endpoint_url
        if credentials_file is not None:
            self.default_credentials_file = credentials_file
        if region is not None:
            self.default_region = region
        if ignore_aws_profile is not None:
            self.ignore_aws_profile = ignore_aws_profile
        self.logger.info("Configured default endpoint/credentials for s5cmd.")

    def _build_base_args(self, endpoint_url: str = None, credentials_file: str = None):
        """Build common s5cmd CLI flags from provided or default values."""
        merged_endpoint = endpoint_url if endpoint_url is not None else self.default_endpoint_url
        merged_credentials = credentials_file if credentials_file is not None else self.default_credentials_file
        base_args = []
        if merged_credentials:
            base_args.extend(["--credentials-file", merged_credentials])
        if merged_endpoint:
            base_args.extend(["--endpoint-url", merged_endpoint])
        return base_args

    def _build_env(self, endpoint_url: str = None, credentials_file: str = None, region: str = None):
        """Build environment variables for s5cmd subprocess to prevent fallback to default AWS profile/region."""
        merged_credentials = credentials_file if credentials_file is not None else self.default_credentials_file
        merged_region = region if region is not None else self.default_region

        env = os.environ.copy()
        if merged_region:
            env["AWS_REGION"] = merged_region
            env["AWS_DEFAULT_REGION"] = merged_region
        if merged_credentials:
            env["AWS_SHARED_CREDENTIALS_FILE"] = merged_credentials
        if self.ignore_aws_profile:
            env.pop("AWS_PROFILE", None)
            env.pop("AWS_CONFIG_FILE", None)
        return env

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
            # Raises an HTTPError if the response status code is 4XX/5XX
            response.raise_for_status()
            with open(self.s5cmd_path, 'wb') as file:
                file.write(response.content)
            # Set executable permissions on Unix-like systems
            if os.name != 'nt':
                os.chmod(self.s5cmd_path, 0o755)
            self.logger.info("s5cmd downloaded and installed successfully.")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download s5cmd: {e}")

    def call_function(self, command: str, *args, capture_output=False, env=None):
        """
        return a process when capture_output is True, otherwise execute and return None.
        """
        # Ensure s5cmd is available before proceeding
        if not self.has_s5cmd():
            self.logger.warning(
                "s5cmd is not available, attempting to download and install.")
            self.get_s5cmd()
            # Recheck if s5cmd is now available, raise error if not
            if not self.has_s5cmd():
                self.logger.error("Failed to ensure s5cmd is available.")
                raise RuntimeError("Failed to ensure s5cmd is available.")

        if capture_output:
            try:
                process = subprocess.Popen(
                    [command, *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
                return process
            except Exception as e:
                self.logger.error(f"Error starting subprocess: {e}")
                return None
        else:
            subprocess.run([command, *args], env=env)

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
        identifier = hashlib.md5(
            f"{num_uris}-{len(first_uri)}-{len(last_uri)}-{len(middle_uri)}-{total_length}".encode()).hexdigest()
        return identifier[:8]

    def generate_s5cmd_file(self, s3_uris: List[str], dest_dir: str):
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

    def _update_progress_bar(self, process, total=None, report_interval: int = 5):
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

    def download_from_s3_list(self, s3_uris: List[str], dest_dir: str, simplified_print: bool = True,
                              endpoint_url: str = None, credentials_file: str = None, region: str = None):

        command_file_path = self.generate_s5cmd_file(s3_uris, dest_dir)

        if simplified_print:
            process = self.call_function(
                self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "run", command_file_path, capture_output=True, env=self._build_env(endpoint_url, credentials_file, region))
            if process and process.stdout:
                self._update_progress_bar(process, total=len(s3_uris))
            else:
                self.logger.error(
                    "Failed to start s5cmd subprocess with output capture.")
        else:
            self.logger.info(f"Generated command file: {command_file_path}")

            self.logger.info(
                f"Downloading {len(s3_uris)} files from S3 to {dest_dir}")
            self.call_function(self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "run", command_file_path, env=self._build_env(endpoint_url, credentials_file, region))
            self.logger.info(
                f"Downloaded {len(s3_uris)} files from S3 to {dest_dir}")

            print(f"removing command file: {command_file_path}")

        # remove the command file after use
        try:
            os.remove(command_file_path)
        except Exception as e:
            self.logger.error(f"Failed to remove command file: {e}")

    def is_local_file(self, path: str):
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

    def download_file(self, uri: str, endpoint_url: str = None, credentials_file: str = None, region: str = None):
        """
        Download a file from a URI (S3 or HTTP/HTTPS URL) to a temporary local path, preserving the filename.
        Args:
            uri (str): The URI of the file to download, can be an S3 URI or a URL.
        Returns:
            str: The local path of the downloaded file.
        """
        if uri.startswith('s3://'):
            local_filename = self.get_filename_from_url(
                uri)  # Use the S3 key as the filename
            local_path = os.path.join('/tmp', local_filename)
            self.call_function(self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "cp", uri, local_path, env=self._build_env(endpoint_url, credentials_file, region))
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

    def mv(self, from_str: str, to_str: str, endpoint_url: str = None, credentials_file: str = None, region: str = None):
        """
        Move a file from a local path to an S3 URI or from an S3 URI to a local path.

        Args:
            from_str (str): The source file path or S3 URI.
            to_str (str): The destination file path or S3 URI.
        """
        self.call_function(self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "mv", from_str, to_str, env=self._build_env(endpoint_url, credentials_file, region))

    def cp(self, from_str: str, to_str: str, simplified_print=False, report_interval=5,
           endpoint_url: str = None, credentials_file: str = None, region: str = None):

        downloaded_path = None
        if re.match(r'https?://', from_str):
            # Download file for HTTP/HTTPS URIs and temporarily save the local path
            downloaded_path = self.download_file(from_str, endpoint_url=endpoint_url, credentials_file=credentials_file, region=region)
            from_str = downloaded_path

        # Warning for uploading a file name instead of into a folder
        if os.path.isfile(from_str) and not to_str.endswith('/'):
            file_extension = os.path.splitext(from_str)[1]
            if file_extension:
                print(
                    f"Warning: '{from_str}' is being uploaded as a file name '{to_str}' instead of into a folder.")

        if downloaded_path:
            # If file was downloaded, decide on the next action based on destination
            if to_str.startswith('s3://'):
                # If destination is S3, use s5cmd to upload
                self.call_function(self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "cp",
                                   downloaded_path, to_str, env=self._build_env(endpoint_url, credentials_file, region))
            else:
                # If destination is local, use shutil.move
                shutil.move(downloaded_path, to_str)
        elif from_str.startswith('s3://') and not to_str.startswith('s3://'):
            # Use s5cmd to copy from S3 to local path directly
            self.call_function(self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "cp", from_str, to_str, env=self._build_env(endpoint_url, credentials_file, region))
        else:
            # For all other cases, including S3 to S3, use call_function directly
            self.call_function(self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "cp", from_str, to_str)

    def run(self, txt_uri: str, simplified_print: bool = True, report_interval: int = 5, total=None,
            endpoint_url: str = None, credentials_file: str = None, region: str = None):
        """
        Run s5cmd with a command file specified by a local path, URL, or S3 URI.

        Args:
            txt_uri (str): The path, URL, or S3 URI of the command file.
            simplified_print (bool): Whether to use simplified progress display.
            total (int, optional): The total number of commands, passed to tqdm for more accurate progress reporting.
        """

        if not self.is_local_file(txt_uri):
            txt_uri = self.download_file(txt_uri, endpoint_url=endpoint_url, credentials_file=credentials_file, region=region)

        process = self.call_function(
            self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "run", txt_uri, capture_output=simplified_print, env=self._build_env(endpoint_url, credentials_file, region))

        if simplified_print and process and process.stdout:
            # Pass total to the progress bar if provided
            self._update_progress_bar(
                process, total=total, report_interval=report_interval)
        elif not simplified_print:
            # The call_function already handled the subprocess.run case
            pass
        else:
            self.logger.error(
                "Failed to start s5cmd subprocess with output capture.")

    def sync(self, source: str, destination: str, simplified_print: bool = True, report_interval: int = 5,
             endpoint_url: str = None, credentials_file: str = None, region: str = None):
        """
        Sync a folder to another folder using s5cmd.

        Args:
            source (str): The source path.  (local or s3)
            destination (str): The destination path.  (local or s3)
            simplified_print (bool): Whether to use simplified progress display.
            report_interval (int): Frequency in seconds to update the progress report.
        """

        # Adjust source path for local folder without trailing slash
        if not source.startswith('s3://') and os.path.isdir(source) and not source.endswith('/'):
            self.logger.warning(
                "Local source path does not end with a slash. Matching s5cmd behavior with `aws s3 cp`.")
            source += '/'
            self.logger.warning(f"Adjusted source path: {source}")

        # Adjust source path for S3 without pattern
        if source.startswith('s3://') and not source.endswith('/*'):
            self.logger.warning("S3 source path does not end with a pattern.")
            source = source.rstrip("/") + '/*'
            self.logger.warning(f"Adjusted source path: {source}")

        # Adjust destination path for S3 without trailing slash
        if destination.startswith('s3://') and not destination.endswith('/'):
            self.logger.warning(
                "S3 destination path does not end with a slash.")
            destination += '/'
            self.logger.warning(f"Adjusted destination path: {destination}")

        process = self.call_function(
            self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "sync", source, destination, capture_output=simplified_print, env=self._build_env(endpoint_url, credentials_file, region))
        if simplified_print and process and process.stdout:
            # For sync, total could potentially be determined by listing source files ahead of time for a more accurate progress bar
            self._update_progress_bar(process, report_interval=report_interval)
        elif not simplified_print:
            # The call_function already handled the subprocess.run case
            pass
        else:
            self.logger.error(
                "Failed to start s5cmd subprocess with output capture.")

    def ls(self, s3_uri: str, report_interval: int = 5,
           endpoint_url: str = None, credentials_file: str = None, region: str = None):
        """
        Lists objects in an S3 bucket and returns details in a structured format.

        Args:
            s3_uri (str): The S3 URI to list objects from.
            report_interval (int): Frequency in seconds to update the progress report.

        Returns:
            dict: A dictionary where keys are the paths of the files, and values are tuples containing size and date.
        """
        process = self.call_function(
            self.s5cmd_path, *self._build_base_args(endpoint_url, credentials_file), "ls", s3_uri, capture_output=True, env=self._build_env(endpoint_url, credentials_file, region))
        if not process:
            self.logger.error(
                "Failed to start s5cmd subprocess with output capture for listing.")
            return {}

        output_dict = {}
        line_count = 0
        start_time = last_report_time = time.time()

        with tqdm(desc="Listing S3 objects") as pbar:
            for line in process.stdout:
                match = re.search(
                    r'^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)$', line.strip())
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
