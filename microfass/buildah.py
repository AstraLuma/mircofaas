"""
An API for buildah
"""
import asyncio
from asyncio import subprocess
import contextlib
import copy
import json
import pathlib
import shutil
from subprocess import CalledProcessError
import urllib.request
import typing


from .utils import AsyncInit

async def _buildah_out(*cmd, **opts):
    """
    Calls buildah, returning the output.

    Returns a str of the stdout or raises a CalledProcessError.
    """
    proc = await asyncio.create_subprocess_exec(
        'buildah', *cmd, encoding='utf-8', stdout=subprocess.PIPE, **opts,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        raise CalledProcessError(
            proc.returncode, ['buildah', *cmd],
            output=stdout, stderr=stderr,
        )
    return stdout

# build-using-dockerfile Build an image using instructions in a Dockerfile

# info                   Display Buildah system information
# version                Display the Buildah version information

# images                 List images in local storage
# containers             List working containers and their base images
# rename                 Rename a container
# push                   Push an image to a specified destination
# login                  Login to a container registry
# logout                 Logout of a container registry


def _dict_diff(old, new):
    """
    Compares two dicts, returning two iterables: the keys that have been added
    or changed, and the keys that have been deleted.
    """
    oldkeys = set(old.keys())
    newkeys = set(new.keys())
    changed = newkeys - oldkeys
    deleted = oldkeys - newkeys
    for key in newkeys & oldkeys:
        if new[key] != old[key]:
            changed.add(key)

    assert len(changed & deleted) == 0
    return changed, deleted


def _join_shellwords(seq):
    """
    Joins a sequence together, parsable https://github.com/mattn/go-shellwords

    This exists because buildah config --cmd uses it
    """
    # FIXME: Handle ' in items
    return " ".join(f"'{s}'" for s in seq)


class Container(metaclass=AsyncInit):
    _id: str

    environ: typing.Dict[str, str]
    command: typing.List[str]
    entrypoint: typing.List[str]
    labels: typing.Dict[str, str]
    volumes: typing.Set[str]
    workdir: str

    def __str__(self):
        return self._id

    def __repr__(self):
        return f'<{type(self).__name__} {self._id}>'

    async def __ainit__(self, image, *, mounts=None):
        args = []
        if mounts:
            for mntinfo in mounts:
                args += ['--volume', ':'.join(map(str, mntinfo))]
        stdout = await _buildah_out('from', *args, str(image))
        self._id = stdout.strip()
        await self._init_config()

    @classmethod
    def _from_id_only(cls, id):
        # Do magic to avoid creating a container
        self = cls.__new__(cls)
        self._id = id
        self._init_config()
        return self

    async def _init_config(self):
        """
        Initialize the config attrs
        """
        info = await self.inspect()
        if info['Config']:
            kinda_config = json.loads(info['Config'])
            config = kinda_config['config']  # Might be 'container_config'??
        else:
            config = {}
        self.environ = dict(
            item.split('=', 1)
            for item in config.get('Env') or {}
        )
        self.command = config.get('Cmd') or []
        self.entrypoint = config.get('Entrypoint') or []
        self.labels = config.get('Labels') or {}
        self.volumes = set(config['Volumes'].keys()) if config.get('Volumes') else set()
        self.workdir = config.get('WorkingDir') or ""
        # TODO: ExposedPorts
        # TODO: StopSignal
        # TODO: Author, comment, created by, domainname, shell, user, workingdir
        # TODO: arch
        self._snapshot_config()

    def _snapshot_config(self):
        """
        Snapshot config for future comparison
        """
        self._snapshot = copy.deepcopy(vars(self))

    def _produce_config_args(self):
        """
        Compares the config attrs to the snapshot and generates args
        """
        args = []

        # Simple stuff: command, entrypoint, workdir
        if self.command != self._snapshot['command']:
            args += ['--cmd', _join_shellwords(self.command)]
        if self.entrypoint != self._snapshot['entrypoint']:
            args += ['--entrypoint', json.dumps(self.entrypoint)]
        if self.workdir != self._snapshot['workdir']:
            args += ['--workingdir', self.workdir]

        # Environment
        env_add, env_del = _dict_diff(self._snapshot['environ'], self.environ)
        for key in env_add:
            args += ['--env', f"{key}={self.environ[key]}"]
        for key in env_del:
            args += ['--env', f"{key}-"]

        # Volumes
        vol_add = self.volumes - self._snapshot['volumes']
        vol_del = self._snapshot['volumes'] - self.volumes
        for v in vol_add:
            args += ['--volume', v]
        for v in vol_del:
            args += ['--volume', f"{v}-"]

        return args

    async def _commit_config(self):
        """
        Commit any config changes to buildah
        """
        if not hasattr(self, '_snapshot'):
            return
        args = self._produce_config_args()
        if args:
            await _buildah_out('config', *args, self._id)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await _buildah_out('rm', self._id, stdout=subprocess.DEVNULL)

    async def inspect(self):
        """
        Return some metadata about the container
        """
        await self._commit_config()
        stdout = await _buildah_out('inspect', '--type', 'container', self._id)
        return json.loads(stdout)

    async def commit(self):
        await self._commit_config()
        stdout = await _buildah_out('commit', self._id)
        return Image._from_id_only(stdout.strip())

    @contextlib.asynccontextmanager
    async def mount(self):
        """
        Mounts the container's filesystem onto the host. Context manager.

        The context manager returns a pathlib.PurePath, which points to the mount
        point.
        """
        stdout = await _buildah_out('mount', self._id)
        path = stdout.strip()
        yield pathlib.PurePath(path)
        await _buildah_out('umount', self._id)

    async def copy_in(self, src, dst):
        """
        Copies a file or directory from the host into the container.

        dst must include the name that will be taken, not just the parent
        directory.

        This is wrong: copy_in("myfile", "/usr/bin")
        This is right: copy_in("myfile", "/usr/bin/foobar")
        """
        await _buildah_out('copy', self._id, str(src), str(dst))

    async def copy_out(self, src, dst):
        """
        Copies a file or directory out of the container to the host.

        dst must include the name that will be taken, not just the parent
        directory.
        """
        dst = pathlib.PurePath(dst)
        # Cleanup what already exists
        if dst.is_dir():
            # FIXME
            shutil.rmtree(dst)
        elif dst.exists():
            dst.unlink()

        async with self.mount() as root:
            fullsrc = root / src.lstrip('/')
            # FIXME
            if fullsrc.is_dir():
                shutil.copytree(fullsrc, dst)
            else:
                shutil.copy2(fullsrc, dst)

    def _run_args(self, user, volumes, mounts, terminal):
        args = []
        if user is not None:
            args += ['--user', str(user)]

        if volumes is not None:
            for vol in volumes:
                if isinstance(vol, str):
                    args += ['--volume', vol]
                else:
                    args += ['--volume', ':'.join(vol)]

        if mounts is not None:
            for mnt in mounts:
                args += [
                    '--mount', ','.join(f"{k}={v}" for k, v in mnt.items())
                ]

        if terminal:
            args += ['--terminal']

        return args

    async def run(
        self, cmd, *,
        # buildah flags
        user=None, volumes=None, mounts=None, terminal=False,
        # TODO: cap add/drop, hostname, ipc, isolation, network, pid, uts
        # Subprocess flags
        stdin=None, input=None, stdout=None, stderr=None, text=None,
        # TODO: cwd, env
    ):
        """
        Runs a command, returning stdout
        """
        self._commit_config()
        args = self._run_args(user, volumes, mounts, terminal)
        opts = {
            'stdin': stdin,
            'input': input,
            'stdout': stdout,
            'stderr': stderr,
            'text': text,
        }

        return await _buildah_out(
            'run', *args, '--', self._id, *cmd,
            encoding='utf-8', **opts,
        )


    async def popen(
        self, cmd, *,
        # buildah flags
        user=None, volumes=None, mounts=None, terminal=False,
        # TODO: cap add/drop, hostname, ipc, isolation, network, pid, uts
        # Subprocess flags
        stdin=None, input=None, stdout=None, stderr=None, text=None,
        # TODO: cwd, env
    ):
        """
        Runs a command, returning the Process
        """
        self._commit_config()
        args = self._run_args(user, volumes, mounts, terminal)
        opts = {
            'stdin': stdin,
            'input': input,
            'stdout': stdout,
            'stderr': stderr,
            'text': text,
        }

        return await asyncio.create_subprocess_exec(
            'buildah', 'run', *args, '--', self._id, *cmd,
            encoding='utf-8', **opts,
        )


    async def popen_with_protocol(
        self, protocol, cmd, *,
        # buildah flags
        user=None, volumes=None, mounts=None, terminal=False,
        # TODO: cap add/drop, hostname, ipc, isolation, network, pid, uts
        # Subprocess flags
        **opts
        # TODO: cwd, env
    ):
        """
        Runs a command using the given Protocol factory.

        Returns (transport, protocol)
        """
        self._commit_config()
        args = self._run_args(user, volumes, mounts, terminal)

        loop = asyncio.get_running_loop()

        code = 'import datetime; print(datetime.datetime.now())'
        exit_future = asyncio.Future(loop=loop)

        # Create the subprocess controlled by DateProtocol;
        # redirect the standard output into a pipe.
        transport, protocol = await loop.subprocess_exec(
            protocol,
            'buildah', 'run', *args, '--', self._id, *cmd,
            **opts)
        return transport, protocol


class ImageNotFoundError(Exception):
    """
    Could not locate the given image
    """


class Image(metaclass=AsyncInit):
    _id: str

    async def __ainit__(self, ident):
        """
        * ident: The hex ID or other identifier of the image

        Will pull the image if not available.
        """
        self._id = await self._resolve(ident)

    def __str__(self):
        return self._id

    def __repr__(self):
        return f'<{type(self).__name__} {self._id}>'

    @classmethod
    def _from_id_only(cls, id):
        # Do magic to avoid extra work, we already know we have an ID
        self = cls.__new__(cls)
        self._id = id
        return self

    async def add_tag(self, tag):
        """
        Add a name to this image.

        If no tag is given, :latest is used.
        """
        await _buildah_out('tag', self._id, tag)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await _buildah_out('rmi', self._id, stdout=subprocess.DEVNULL)

    async def inspect(self):
        """
        Return some metadata about the image
        """
        stdout = await _buildah_out('inspect', '--type', 'image', self._id)
        return json.loads(stdout)

    @classmethod
    async def list(cls, name=None, *, all=False):
        """
        Lists images available locally.

        If a name is given, only list ((something something)).

        If all is True, also include intermediate build images.
        """
        cmd = ['images', '--json']
        if all:
            cmd += ['--all']
        if name:
            cmd += [name]
        stdout = await _buildah_out(*cmd)

        # TODO: Add a way to get the Image from each item
        yield from json.loads(stdout)

    @classmethod
    async def _resolve(cls, id):
        if any(img['id'] == id for img in cls.list()):
            return id

        # TODO: Use `buildah inspect` to test if it's available locally.
        try:
            stdout = await _buildah_out('pull', '--quiet', id, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as exc:
            raise ImageNotFoundError(f"Could not find image {id}") from exc
        else:
            return stdout.strip()

    @classmethod
    async def pull(cls, name):
        """
        Pulls down the given image.
        """
        try:
            stdout = await _buildah_out('pull', name)
        except subprocess.CalledProcessError as exc:
            raise ImageNotFoundError(f"Could not find image {name}") from exc
        else:
            id = stdout.strip()
            return cls._from_id_only(id)
