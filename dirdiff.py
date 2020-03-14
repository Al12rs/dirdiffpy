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

from dirtools import Dir, DirState, compute_diff
import json
import xxhash

def _xxhash_file(filepath, blocksize=4096):
    xxhash64 = xxhash.xxh64()
    with open(filepath, 'rb') as fp:
        while 1:
            data = fp.read(blocksize)
            if data:
                xxhash64.update(data)
            else:
                break
    return xxhash64

def xxhash_file(filepath, blocksize=4096):
    hash = _xxhash_file(filepath, blocksize)
    return hash.hexdigest()

d = Dir("C:\\Modding\\WJModlists\\NOISE\\mods\\Interesting NPCs SE")
dir_state = DirState(d, None, xxhash_file)

#with open("./3dnpc_state.json", 'w') as f:
#            f.write(json.dumps(dir_state.state))

old_state = DirState(d, DirState.from_json('./out/3dnpc_state.json').state, xxhash_file)

diff = dir_state - old_state

with open("./out/diff.json", 'w') as f:
            f.write(json.dumps(diff))
