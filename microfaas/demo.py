import asyncio
import sys

from microfaas.runtime import Runtime


async def run(bundle, func, body):
    async with Runtime(bundle) as rt:
        await rt.mk_call(func, body)

if __name__ == '__main__':
    asyncio.run(run(*sys.argv[1:]))
