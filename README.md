# s5cmd-python

**updated doc: check lark**: https://mufo9rl7c6.larksuite.com/wiki/DvgPwF3gKifmOWk2hNnuTkXXsVh?from=from_copylink


python binding for using s5cmd to download and upload files to s3 efficiently

The `S5CmdRunner` class provides a Python interface for interacting with `s5cmd`, a command-line tool designed for efficient data transfer to and from Amazon S3.

For more information about s5cmd, please refer to the original [s5cmd](https://github.com/peak/s5cmd) repository.

## Features

- Check for the presence of `s5cmd` and download it if necessary.
- Execute `s5cmd` commands `cp`, `mv`, and `run`.
- Handle file downloads from URLs and S3 URIs.
- Generate command files for batch operations with `s5cmd`.
- Simplify operations like copying and moving files between local paths and S3 URIs.

## Installation

To use `S5CmdRunner`, ensure that Python 3.10 or higher is installed. The project itself can be installed from pip:

```bash
pip install s5cmdpy
```

or from source:

```bash
git clone <repo url>
cd s5cmd-python
pip install -e .
```

## Usage

Here are some examples of how to use the `S5CmdRunner` class:

### Initialize S5CmdRunner

```python
from s5cmdpy import S5CmdRunner
runner = S5CmdRunner()
```

### Run s5cmd with a Local Command File

```python
# local_txt: `cp s3://dataset-artstation-uw2/artists/__andrey__/1841730##GZGgW.json .`
local_txt_path = "s5cmd_test.txt"
runner.run(local_txt_path)
```

### Run s5cmd with a Command File from S3

```python
# Useful in environments like SageMaker or for reproducibility; 
# Extends `s5cmd run something.txt` to support command files stored in S3
txt_s3_uri = "s3://dataset-artstation-uw2/s5cmd_test.txt"
runner.run(txt_s3_uri)
```

### Download Multiple Files from S3

```python
# Input a series of S3 URIs to create the necessary commands.txt for `s5cmd run`, 
# then execute `s5cmd run <commands.txt>`

s3_uris = [
    's3://dataset-artstation-uw2/artists/__andrey__/1841730##GZGgW.json', 
    's3://dataset-artstation-uw2/artists/__andrey__/2249992##q5Y22.json'
]
destination_dir = '/home/ubuntu/datasets/s5cmd_test'
runner.download_from_s3_list(s3_uris, destination_dir)
```

### Download a file from internet and upload to S3
`cp` command also works with a file from internet:
```python
# Download a file from internet and upload to S3
target_url = "https://huggingface.co/kiriyamaX/mld-caformer/resolve/main/ml_caformer_m36_dec-5-97527.onnx"
dst_s3_uri = "s3://dataset-artstation-uw2/_dev/"

runner.cp(target_url, dst_s3_uri)
```

## License

S5cmd itself is MIT licensed. This project is also MIT licensed.

