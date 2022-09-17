import os
import asyncio
import aiohttp
from .base import _Base
from .drive import _Drive
from typing import Optional


class Deta:

    def __init__(
            self,
            project_key: Optional[str] = None,
            *,
            session: Optional[aiohttp.ClientSession] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self.token = project_key or os.getenv('DETA_PROJECT_KEY')
        assert self.token, 'project key is required'
        if not session:
            self.session = aiohttp.ClientSession(loop=loop)
        else:
            self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()
        if exc:
            raise exc

    async def close(self):
        await self.session.close()

    def base(self, name: str) -> _Base:
        return _Base(name, self.token, self.session)

    def drive(self, name: str) -> _Drive:
        return _Drive(name, self.token, self.session)
