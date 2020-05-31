"""
Manages the environment that runs bundles.
"""
import asyncio
import importlib.resources
import logging
import zipfile

from urp.client import ClientSubprocessProtocol, Disconnected

from .buildah import Container


LOG = logging.getLogger(__name__)


class Runtime:
    """
    Manages the container and presents the interface for connections to call
    """
    def __init__(self, source):
        self.zipsource = zipfile.ZipFile(source)
        self.call_lock = asyncio.Lock()

    async def __aenter__(self):
        self.container = await self._setup_container()
        self.client = None
        start_event = asyncio.Event()
        self.task = asyncio.create_task(self._starter_task(start_event))
        await start_event.wait()
        return self

    async def __aexit__(self, *exc):
        self.task.cancel()
        await self.container.__aexit__(*exc)

    async def _setup_container(self):
        loop = asyncio.get_running_loop()

        cont = await Container('python:3')
        # TODO: Data volume
        await cont.__aenter__()
        try:
            async with cont.mount() as root:
                await loop.run_in_executor(None, (root / 'app').mkdir)
                await loop.run_in_executor(None, self.zipsource.extractall, root)

            cont.workdir = '/app'

            await cont.run(['pip', 'install', 'unnamed-rpc'], stdout=None)

            with importlib.resources.path('microfaas', '__runner__.py') as src:
                await cont.copy_in(src, '/__runner__.py')
        except:
            await cont.__aexit__(None, None, None)
            raise
        else:
            return cont

    async def _starter_task(self, start_event):
        while True:
            try:
                transpo, self.client = await self.container.popen_with_protocol(
                    ClientSubprocessProtocol,
                    ['python', '/__runner__.py'],
                )
                start_event.set()
                await self.client.finished()
            except:
                await self.client.close()
                raise
            else:
                LOG.info("Inner process exited rc=%s", transpo.get_returncode())
                # TODO: Backoff policy

    async def mk_call(self, func, body, **extra_data):
        async with self.call_lock:
            while True:
                try:
                    async for resp in self.client[func](_=body, **extra_data):
                        if isinstance(resp, Exception):
                            LOG.error("Received error: %s", resp)
                except Exception:
                    LOG.exception("Error calling %s", func)
                    # Try again after yielding
                    await asyncio.sleep(0.001)
                    # TODO: Backoff policy
                    continue
                else:
                    break
