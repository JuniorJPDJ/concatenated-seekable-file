import io

import pytest

from concatenated_seekable_file.AsyncConcatenatedSeekableFile import AsyncConcatenatedSeekableFile


@pytest.mark.asyncio
async def test_async():
    in_f = [io.BytesIO(b'test'), io.BytesIO(b' KURWA\n'), io.BytesIO(b'kek')]

    async with AsyncConcatenatedSeekableFile(*in_f) as acsf:
        assert sum(len(f.getbuffer()) for f in in_f) == len(acsf)

        assert await acsf.tell() == 0
        assert await acsf.read1(1) == b't'
        assert await acsf.tell() == 1

        # middle of the first file
        assert await acsf.seek(2) == 2
        assert acsf._file_index == 0
        assert acsf._file_pos == in_f[0].tell() == 2
        assert await acsf.read1() == b'st'

        # last byte of the first file
        assert await acsf.seek(3) == 3
        assert acsf._file_index == 0
        assert acsf._file_pos == in_f[0].tell() == 3
        assert await acsf.read1(1) == b't'

        # first byte of the second file
        assert await acsf.seek(4) == 4
        assert acsf._file_index == 1
        assert acsf._file_pos == in_f[1].tell() == 0
        assert await acsf.read1(1) == b' '

        # middle of the second file
        assert await acsf.seek(6) == 6
        assert acsf._file_index == 1
        assert acsf._file_pos == in_f[1].tell() == 2
        assert await acsf.read1(1) == b'U'

        # first byte of the last file
        assert await acsf.seek(11) == 11
        assert acsf._file_index == 2
        assert acsf._file_pos == in_f[2].tell() == 0
        assert await acsf.read1(1) == b'k'

        # before a last byte in the last file
        assert await acsf.seek(13) == 13
        assert acsf._file_index == 2
        assert acsf._file_pos == in_f[2].tell() == 2
        assert await acsf.read1(1) == b'k'

        # after a last byte in the last file
        assert await acsf.seek(14) == 14
        assert acsf._file_index == 2
        assert acsf._file_pos == in_f[2].tell() == 3
        assert await acsf.read1(1) == b''

        # even more in void
        assert await acsf.seek(16) == 16
        assert acsf._file_index == 2
        assert acsf._file_pos == in_f[2].tell() == 5
        assert await acsf.read1(1) == b''

        # read all files using only one underlying `read()` call
        assert await acsf.seek(1) == 1
        assert await acsf.read1() == b'est'
        assert await acsf.read1() == b' KURWA\n'
        assert await acsf.read1() == b'kek'

        # read into bytearray
        await acsf.seek(0)
        ba = bytearray(10)
        assert await acsf.readinto(ba) == 10
        assert ba == b'test KURWA'
        assert await acsf.readinto(ba) == 4
        assert ba == b'\nkek KURWA'

        # read whole file into bytearray
        await acsf.seek(0)
        ba = bytearray(16)
        assert await acsf.readinto(ba) == 14
        assert b'test KURWA\nkek\0\0' == ba

        # read something
        await acsf.seek(3)
        assert await acsf.read(5) == b't KUR'
        assert await acsf.read() == b'WA\nkek'

        # read everything
        await acsf.seek(0)
        assert await acsf.readall() == b'test KURWA\nkek'

        # read lines
        await acsf.seek(0)
        assert await acsf.readline(6) == b'test K'
        assert await acsf.readline(6) == b'URWA\n'
        assert await acsf.readline(6) == b'kek'

        await acsf.seek(0)
        assert await acsf.readline() == b'test KURWA\n'
        assert await acsf.read() == b'kek'

        await acsf.seek(0)
        assert await acsf.readlines() == [b'test KURWA\n', b'kek']

        # read lines emulating not seekable file
        async def nope(*args, **kwargs):
            return False

        await acsf.seek(0)

        _seekable = acsf.seekable
        acsf.seekable = nope

        assert await acsf.readline(6) == b'test K'
        assert await acsf.readline(6) == b'URWA\n'
        assert await acsf.readline(6) == b'kek'

        acsf.seekable = _seekable
        await acsf.seek(0)
        acsf.seekable = nope

        assert await acsf.readline() == b'test KURWA\n'
        assert await acsf.read() == b'kek'
