import asyncio
import contextlib
import dataclasses
import logging
import typing

from .runtime import Runtime

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Bundle:
    """
    Holds a bunch of live objects the manager has to keep track of.
    """
    #: The source bundle, can be filename, path-like, or file-like
    # bundle: typing.Any
    #: The current runtime
    runtime: Runtime
    #: The call queue
    queue: asyncio.Queue
    #: The task processing the queue
    task: asyncio.Task


class Manager:
    #: Holds all the metadata about our deployed bundles
    bundles: typing.Dict[str, Bundle]

    def __init__(self):
        self.bundles = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        """
        This immediately exits, stopping tasks and freeing containers.
        """
        # Stop all queue processing tasks
        tasks = []
        for bdata in self.bundles.values():
            if bdata.task is None:
                continue
            tasks.append(bdata.task)
            bdata.task.cancel()
        # Wait for the tasks to actually exit
        for task in asyncio.as_completed(tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                LOG.exception("Error stopping task %r", task)
        # Clean up the containers
        for name, bdata in self.bundles.items():
            try:
                await bdata.runtime.__aexit__(*exc)
            except Exception:
                LOG.exception("Error cleaning up runtime for  %s", name)

    async def join(self):
        """
        Block until all the queues are empty.

        Note that this does not prevent adding new jobs or any such.
        """
        await asyncio.gather(*(bdata.queue.join() for bdata in self.bundles.values()))

    async def deploy(self, name, bundle):
        """
        Deploy a new bundle at name.

        When this function returns, the bundle will be fully deployed and
        operating.

        If name didn't previously exist, new queues, containers, etc will be
        created.

        If name did exist, only the container will be replaced. Any unprocessed
        items in the queue will be handled by the new deployment.
        """
        old_runtime = None
        if name in self.bundles:
            # Replacement deploy
            bdata = self.bundles[name]
            bdata.bundle = bundle
            new_runtime = Runtime(bundle)
            await new_runtime.__aenter__()
            # New runtime ready to accept jobs, swap runtimes
            old_runtime, bdata.runtime = bdata.runtime, new_runtime
            # This is so that we transparently swap the current runtime without
            # restarting the queue-processing task.

            # Clean up old
            try:
                await old_runtime.__aexit__(None, None, None)
            except Exception:
                LOG.exception("Error cleaning up old runtime of %s", name)
        else:
            # New deploy
            bdata = self.bundles[name] = Bundle(
                # bundle=bundle,
                queue=asyncio.Queue(),
                runtime=Runtime(bundle),
                task=None,  # Later
            )
            # Prepare container
            await bdata.runtime.__aenter__()
            # Start queue consumer
            bdata.task = asyncio.create_task(self._loop_on_jobs(name), name=f"{bundle}-queue-processor")

    async def _loop_on_jobs(self, bundle_name):
        """
        Consumes a queue, processing each item in turn.

        The items must be three-tuples of (func, body, extras).

        * func: str: the function to call
        * body: JSON-ish: the body of the event
        * extras: dict[str, JSON-ish]: extra data for the event
        """
        while True:
            try:
                bundle = self.bundles[bundle_name]
            except KeyError:
                # The bundle got deleted, just exit
                return

            # This is to allow some of the objects to get swapped out as needed
            q = bundle.queue
            func, body, extras = await q.get()
            await bundle.runtime.do_call(func, body, **extras)
            q.task_done()

    async def delete(self, name, *, join=False):
        """
        Deletes a bundle, cleaning up all its resources.

        If join is True, wait until the previously queued items are processed
        before cleaning up. New items will be prevented from being added.

        If join is False (default), previously queued items will be discarded.
        """
        try:
            bdata = self.bundles.pop(name)
        except KeyError as exc:
            raise ValueError(f"Bundle {name} does not exist") from exc

        if join:
            await bdata.queue.join()

        # Stop the queue processing task
        bdata.task.cancel()
        try:
            await bdata.task
        except asyncio.CancelledError:
            pass
        except Exception:
            LOG.exception("Error stopping task %r for bundle %s", task, name)

        # Clean up the container
        try:
            await bdata.runtime.__aexit__(None, None, None)
        except Exception:
            LOG.exception("Error cleaning up runtime for  %s", name)

    async def call_func(self, bundle_name, function, body, **extras):
        """
        Calls the given function inside the given bundle with the body and extra
        data.

        This is enqueued, not immediate.

        function is in the form of pkgutil.resolve_name(): Either
        pkg.module.function or pkg.module:function.
        """
        try:
            bdata = self.bundles[bundle_name]
        except KeyError as exc:
            raise ValueError(f"Unable to find bundle {bundle_name}") from exc

        await bdata.queue.put((function, body, extras))

    def __iter__(self):
        """
        Get the names of all currently-running bundles.

        To be clear:
        * New bundles being deployed but not ready: Not included
        * Bundles that are being replaced: Included
        * Bundles that are operating normally: Included
        * Bundles that are being cleaned up after deletion: Not included
        """
        yield from self.bundles.keys()
