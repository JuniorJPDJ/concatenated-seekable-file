import asyncio
import inspect
import io
import os
from typing import Callable, Optional, Awaitable, TypeVar, Union, Tuple, List

import asyncio_rlock
from asynciobase import AsyncIOBase

T = TypeVar('T')


# noinspection SpellCheckingInspection
async def _acall(f: Callable[..., Union[Awaitable[T], T]], *args, **kwargs) -> T:
    # detects if returned value is awaitable and awaits if needed
    ret = f(*args, **kwargs)
    if inspect.isawaitable(ret):
        ret = await ret
    return ret


# noinspection SpellCheckingInspection
async def _flen(f: io.IOBase) -> Optional[int]:
    if hasattr(f, '__len__'):
        # noinspection PyTypeChecker
        return len(f)

    elif hasattr(f, 'len'):
        return await _acall(f.len)

    elif hasattr(f, 'getbuffer'):
        # BytesIO and similar
        return len(await _acall(f.getbuffer))

    elif hasattr(f, 'fileno'):
        # real files
        try:
            fileno = await _acall(f.fileno)
        except OSError:
            pass
        else:
            if hasattr(f, 'mode'):
                if 'b' not in f.mode:
                    # text mode files are not supported
                    return None
            return os.fstat(fileno).st_size

    elif hasattr(f, 'seek') and hasattr(f, 'tell'):
        # seekable files fallback
        try:
            offset = await _acall(f.tell)
            length = await _acall(f.seek, 0, io.SEEK_END)
            await _acall(f.seek, offset, io.SEEK_SET)
            return length
        except OSError:
            pass

    return None


class AsyncConcatenatedSeekableFile(AsyncIOBase):
    async def __aenter__(self):
        async with self._lock:
            if not self.__inited:
                self.lengths = await self._call_on_all_files(_flen)

                for i in range(len(self.lengths)):
                    if self.lengths[i] is None:
                        raise ValueError(f'{self.files[i]} ({i}) has unknown length.')

                self._closed = False
                self.__inited = True
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        async with self._lock:
            await self.close()

    def __init__(self, *files: Tuple[io.IOBase], name: str = ''):
        # SHOULD BE CREATED USING ASYNC CONTEXT MANAGER OR `create()` METHOD
        self.__inited = False

        self._lock = asyncio_rlock.RLock()
        self._pos = 0
        self._file_index = 0
        self._file_pos = 0
        self._closed = True
        self._name = name

        self.files = files
        self.lengths = None

    def __len__(self) -> int:
        return sum(self.lengths)

    async def _call_on_all_files(self, f: Callable[[io.IOBase], Union[T, Awaitable[T]]]) -> Tuple[T, ...]:
        return await asyncio.gather(*(_acall(f, file) for file in self.files))

    async def _recalc_file(self):
        async with self._lock:
            pos = self._pos
            self._file_index = 0
            self._file_pos = 0

            # find file on offset
            while pos >= self.lengths[self._file_index] and self._file_index + 1 < len(self.files):
                pos -= self.lengths[self._file_index]
                self._file_index += 1

            # handle last file smaller than pos
            fpos = await _acall(self.files[self._file_index].seek, pos, io.SEEK_SET)
            if fpos != pos:
                diff = pos - fpos
                self._pos -= diff

            self._file_pos = fpos

    @property
    def _file(self) -> io.IOBase:
        return self.files[self._file_index]

    @property
    def _file_length(self) -> int:
        return self.lengths[self._file_index]

    async def close(self):
        async with self._lock:
            await self._call_on_all_files(lambda f: f.close())
            self._closed = True

    @property
    def closed(self):
        return self._closed

    @classmethod
    async def create(cls, *files: io.IOBase, name: str = '') -> 'AsyncConcatenatedSeekableFile':
        self = cls(*files, name=name)
        await self.__aenter__()
        return self

    @property
    def name(self) -> str:
        return self._name

    async def read(self, amount=-1) -> bytes:
        if amount < 0 or amount > len(self) - self._pos:
            amount = len(self) - self._pos

        ba = bytearray(amount)
        await self.readinto(ba)
        return bytes(ba)

    async def read1(self, amount=-1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        elif not await self.readable():
            raise io.UnsupportedOperation

        async with self._lock:
            fpos = self._file_pos
            val = await _acall(self._file.read, amount)

            # check if data not larger than max expected data length and cut
            max_len = max(self._file_length - fpos, 0)
            if len(val) > max_len:
                val = val[:max_len]

            self._file_pos = await _acall(self._file.tell)
            self._pos += len(val)

            await self._recalc_file()

            return val

    async def readable(self) -> bool:
        async with self._lock:
            return not self.closed and all(await self._call_on_all_files(lambda f: f.readable()))

    async def readinto(self, buffer) -> int:
        async with self._lock:
            buf_len = len(buffer)
            data_len = 0
            while data_len < buf_len:
                data = await self.read1(buf_len - data_len)

                # detect eof
                if data == b'':
                    break

                buffer[data_len:data_len+len(data)] = data
                data_len += len(data)

            return data_len

    async def seek(self, offset, whence=io.SEEK_SET) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not await self.seekable():
            raise io.UnsupportedOperation

        async with self._lock:
            # calculate position from start
            if whence == io.SEEK_SET:
                self._pos = 0
            elif whence == io.SEEK_CUR:
                pass
            elif whence == io.SEEK_END:
                self._pos = len(self)
            else:
                raise ValueError("whence must be io.SEEK_SET (0), "
                                 "io.SEEK_CUR (1) or io.SEEK_END (2)")

            self._pos += offset

            await self._recalc_file()

            return self._pos

    async def seekable(self) -> bool:
        async with self._lock:
            return all(await self._call_on_all_files(lambda f: f.seekable()))

    async def tell(self) -> int:
        async with self._lock:
            return self._pos
