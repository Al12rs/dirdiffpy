"""Copyright (c) 2020 AL, hjk

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from typing import Any, Callable, Dict, List
import xxhash
import os
import hashlib
import json

IndexerType = Dict[str, Callable[[str], Any]]

class FileIndexers:
    """
    """

    @staticmethod
    def sha256_file(filepath:str, blocksize:int=4096) -> str:
        """
        """
        sha = hashlib.sha256()
        with open(filepath, 'rb') as fp:
            while 1:
                data = fp.read(blocksize)
                if data:
                    sha.update(data)
                else:
                    break
        return sha.hexdigest()


    @staticmethod
    def xxhash_file(filepath:str, blocksize:int=4096) -> str:
        xxhash64 = xxhash.xxh64()
        with open(filepath, 'rb') as fp:
            while 1:
                data = fp.read(blocksize)
                if data:
                    xxhash64.update(data)
                else:
                    break
        return xxhash64.hexdigest()

    @classmethod
    def XXHASH64(cls) -> IndexerType:
        return { "xxhash" : cls.xxhash_file }

    @classmethod
    def GETMTIME(cls) -> IndexerType:
        return { "getmtime" : os.path.getmtime }

    @classmethod
    def SHA256(cls) -> IndexerType:
        return { "sha256" : cls.sha256_file }

def dirSnapshot(targetDir:str, 
                file_indexers:List[IndexerType] = 
                [FileIndexers.XXHASH64()],
                dir_indexers:List[IndexerType]=[]):
    """
    """