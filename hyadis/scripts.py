import os

import click
import ray
import ray.scripts.scripts
from click.decorators import FC, _param_memo
from ray.autoscaler._private.cli_logger import add_click_logging_options

from .api import disable_hyadis, enable_hyadis
from .utils.constants import SCHEDULER_PORT_ENV_VAR

cli = click.Group()


def _inherit_cli_option(command: click.core.Command):

    def decorator(f: FC):
        for param in command.params:
            _param_memo(f, param)
        return f

    return decorator


@cli.command()
@_inherit_cli_option(ray.scripts.scripts.start)
@click.option(
    "--scheduler-port",
    required=False,
    type=int,
    help="Port number for the scheduler service.",
)
@add_click_logging_options
def start(head, scheduler_port=None, **kwargs):
    enable_hyadis()
    if scheduler_port is not None:
        os.environ[SCHEDULER_PORT_ENV_VAR] = str(scheduler_port)
    else:
        assert head, "Please specify --scheduler-port to connect to the scheduler service on the head node."
    ctx = click.get_current_context()
    kwargs["head"] = head
    ctx.invoke(ray.scripts.scripts.start, **kwargs)


@cli.command()
@_inherit_cli_option(ray.scripts.scripts.stop)
@add_click_logging_options
def stop(**kwargs):
    ctx = click.get_current_context()
    ctx.invoke(ray.scripts.scripts.stop, **kwargs)
    disable_hyadis()


def main():
    cli()


if __name__ == "__main__":
    main()
