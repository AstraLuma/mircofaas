import asyncio
import sys

from microfaas.manager import Manager


async def run(bundle, func, body):
    async with Manager() as man:
        print("Deploying")
        await man.deploy("demo", bundle)
        print("Calling")
        await man.call_func("demo", func, body)
        print("Waiting")
        await man.join()
        print("Deleting")
        await man.delete("demo")
        print("Done")

if __name__ == '__main__':
    asyncio.run(run(*sys.argv[1:]))
