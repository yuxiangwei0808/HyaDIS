import os

import ray
import ray.ray_constants as ray_constants
from hyadis.utils.constants import PROCESS_TYPE_SCHEDULER, SCHEDULER_PORT_ENV_VAR

from ..utils import get_logger
from .services import start_hyadis_raylet, start_hyadis_scheduler

ray_node_cls = ray.node.Node


class HyadisNode(ray_node_cls):

    def __init__(self, ray_params, head=False, **kwargs) -> None:
        if not head:
            scheduler_port = os.getenv(SCHEDULER_PORT_ENV_VAR)
            assert int(scheduler_port) > 0
            self._scheduler_address = ":".join((ray_params.gcs_address.rpartition(":")[0], scheduler_port))
            self.validate_ip_port(self.scheduler_address)
        super().__init__(ray_params, head=head, **kwargs)

    @property
    def scheduler_address(self):
        """Get the global scheduler address."""
        assert self._scheduler_address is not None, "Global scheduler address is not set"
        return self._scheduler_address

    def start_head_processes(self):
        super().start_head_processes()
        self.start_scheduler()

    def start_scheduler(self):
        stdout_file, stderr_file = self.get_log_file_handles("scheduler", unique=True)
        scheduler_port = os.getenv(SCHEDULER_PORT_ENV_VAR)
        if scheduler_port is not None:
            scheduler_port = int(scheduler_port)
        if scheduler_port is None or scheduler_port == 0:
            scheduler_port = self._get_cached_port("scheduler_port")
        process_info = start_hyadis_scheduler(self.redis_address,
                                              self.gcs_address,
                                              self._logs_dir,
                                              self._node_ip_address,
                                              scheduler_port,
                                              metrics_agent_port=self._ray_params.metrics_agent_port,
                                              stdout_file=stdout_file,
                                              stderr_file=stderr_file,
                                              redis_password=self._ray_params.redis_password,
                                              config=self._config,
                                              fate_share=self.kernel_fate_share)
        self.all_processes[PROCESS_TYPE_SCHEDULER] = [process_info]
        self._scheduler_address = f"{self._node_ip_address}:" f"{scheduler_port}"
        logger = get_logger()
        logger.info(f"HyaDIS scheduler started at {self._scheduler_address}")

    def start_raylet(
        self,
        plasma_directory,
        object_store_memory,
        use_valgrind=False,
        use_profiler=False,
    ):
        stdout_file, stderr_file = self.get_log_file_handles("raylet", unique=True)
        process_info = start_hyadis_raylet(
            self.redis_address,
            self.gcs_address,
            self.scheduler_address,
            self._node_ip_address,
            self._ray_params.node_manager_port,
            self._raylet_socket_name,
            self._plasma_store_socket_name,
            self._ray_params.worker_path,
            self._ray_params.setup_worker_path,
            self._ray_params.storage,
            self._temp_dir,
            self._session_dir,
            self._runtime_env_dir,
            self._logs_dir,
            self.get_resource_spec(),
            plasma_directory,
            object_store_memory,
            min_worker_port=self._ray_params.min_worker_port,
            max_worker_port=self._ray_params.max_worker_port,
            worker_port_list=self._ray_params.worker_port_list,
            object_manager_port=self._ray_params.object_manager_port,
            redis_password=self._ray_params.redis_password,
            metrics_agent_port=self._ray_params.metrics_agent_port,
            metrics_export_port=self._metrics_export_port,
            dashboard_agent_listen_port=self._ray_params.dashboard_agent_listen_port,
            use_valgrind=use_valgrind,
            use_profiler=use_profiler,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            config=self._config,
            huge_pages=self._ray_params.huge_pages,
            fate_share=self.kernel_fate_share,
            socket_to_use=None,
            max_bytes=self.max_bytes,
            backup_count=self.backup_count,
            start_initial_python_workers_for_first_job=self._ray_params.start_initial_python_workers_for_first_job,
            ray_debugger_external=self._ray_params.ray_debugger_external,
            env_updates=self._ray_params.env_vars,
            node_name=self._ray_params.node_name,
        )
        assert ray_constants.PROCESS_TYPE_RAYLET not in self.all_processes
        self.all_processes[ray_constants.PROCESS_TYPE_RAYLET] = [process_info]
        logger = get_logger()
        logger.info("HyaDIS raylet started")

    def kill_scheduler(self, check_alive=True):
        self._kill_process_type(PROCESS_TYPE_SCHEDULER, check_alive=check_alive)

    def kill_all_processes(self, check_alive=True, allow_graceful=False):
        super().kill_all_processes(check_alive=check_alive, allow_graceful=allow_graceful)
        if PROCESS_TYPE_SCHEDULER in self.all_processes:
            self._kill_process_type(PROCESS_TYPE_SCHEDULER, check_alive=check_alive, allow_graceful=allow_graceful)


def enable_hyadis_node():
    ray.node.Node = HyadisNode


def disable_hyadis_node():
    ray.node.Node = ray_node_cls
