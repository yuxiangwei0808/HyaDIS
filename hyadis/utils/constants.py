from pathlib import Path
import os

from ray.autoscaler._private.constants import RAY_PROCESSES

PROCESS_TYPE_SCHEDULER = "scheduler"

HYADIS_PATH = Path(__file__).parent.absolute().parent.parent

RAYLET_EXECUTABLE = os.path.join(HYADIS_PATH, "bazel-bin/raylet")
SCHEDULER_EXECUTABLE = os.path.join(HYADIS_PATH, "bazel-bin/scheduler")

RAY_PROCESSES.insert(0, [PROCESS_TYPE_SCHEDULER, True])

SCHEDULER_PORT_ENV_VAR = "HYADIS_SCHEDULER_PORT"
