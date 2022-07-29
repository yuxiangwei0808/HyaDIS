import multiprocessing
import os
import subprocess
import sys

import ray
import ray.ray_constants as ray_constants
from hyadis.utils.constants import PROCESS_TYPE_SCHEDULER, RAYLET_EXECUTABLE, SCHEDULER_EXECUTABLE
from ray._private.services import DEFAULT_NATIVE_LIBRARY_PATH, RAY_PATH, start_ray_process


def start_hyadis_scheduler(redis_address,
                           gcs_address,
                           log_dir,
                           node_ip_address,
                           scheduler_port,
                           metrics_agent_port=None,
                           stdout_file=None,
                           stderr_file=None,
                           redis_password=None,
                           config=None,
                           fate_share=None):
    assert scheduler_port > 0

    command = [
        SCHEDULER_EXECUTABLE,
        f"--log_dir={log_dir}",
        f"--gcs_address={gcs_address}",
        f"--node_ip_address={node_ip_address}",
        f"--scheduler_port={scheduler_port}",
        f"--metrics-agent-port={metrics_agent_port}",
    ]
    if redis_address:
        redis_ip_address, redis_port = redis_address.rsplit(":")
        command += [
            f"--redis_address={redis_ip_address}",
            f"--redis_port={redis_port}",
        ]
    if redis_password:
        command += [f"--redis_password={redis_password}"]
    process_info = start_ray_process(
        command,
        PROCESS_TYPE_SCHEDULER,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
    )
    return process_info


def start_hyadis_raylet(redis_address,
                        gcs_address,
                        scheduler_address,
                        node_ip_address,
                        node_manager_port,
                        raylet_name,
                        plasma_store_name,
                        worker_path,
                        setup_worker_path,
                        storage,
                        temp_dir,
                        session_dir,
                        resource_dir,
                        log_dir,
                        resource_spec,
                        plasma_directory,
                        object_store_memory,
                        min_worker_port=None,
                        max_worker_port=None,
                        worker_port_list=None,
                        object_manager_port=None,
                        redis_password=None,
                        metrics_agent_port=None,
                        metrics_export_port=None,
                        dashboard_agent_listen_port=None,
                        use_valgrind=False,
                        use_profiler=False,
                        stdout_file=None,
                        stderr_file=None,
                        config=None,
                        huge_pages=False,
                        fate_share=None,
                        socket_to_use=None,
                        start_initial_python_workers_for_first_job=False,
                        max_bytes=0,
                        backup_count=0,
                        ray_debugger_external=False,
                        env_updates=None,
                        node_name=None):
    assert node_manager_port is not None and type(node_manager_port) == int

    if use_valgrind and use_profiler:
        raise ValueError("Cannot use valgrind and profiler at the same time.")

    assert resource_spec.resolved()
    static_resources = resource_spec.to_resource_dict()

    # Limit the number of workers that can be started in parallel by the
    # raylet. However, make sure it is at least 1.
    num_cpus_static = static_resources.get("CPU", 0)
    maximum_startup_concurrency = max(1, min(multiprocessing.cpu_count(), num_cpus_static))

    # Format the resource argument in a form like 'CPU,1.0,GPU,0,Custom,3'.
    resource_argument = ",".join(["{},{}".format(*kv) for kv in static_resources.items()])

    # TODO: Hyadis currently may do not need java or cpp workers.
    # Will update them in the future.

    # has_java_command = False
    # if shutil.which("java") is not None:
    #     has_java_command = True

    # ray_java_installed = False
    # try:
    #     jars_dir = get_ray_jars_dir()
    #     if os.path.exists(jars_dir):
    #         ray_java_installed = True
    # except Exception:
    #     pass

    # include_java = has_java_command and ray_java_installed
    # if include_java is True:
    #     java_worker_command = build_java_worker_command(
    #         gcs_address,
    #         plasma_store_name,
    #         raylet_name,
    #         redis_password,
    #         session_dir,
    #         node_ip_address,
    #         setup_worker_path,
    #     )
    # else:
    #     java_worker_command = []

    # if os.path.exists(DEFAULT_WORKER_EXECUTABLE):
    #     cpp_worker_command = build_cpp_worker_command(
    #         gcs_address,
    #         plasma_store_name,
    #         raylet_name,
    #         redis_password,
    #         session_dir,
    #         log_dir,
    #         node_ip_address,
    #     )
    # else:
    #     cpp_worker_command = []

    # Create the command that the Raylet will use to start workers.
    # TODO(architkulkarni): Pipe in setup worker args separately instead of
    # inserting them into start_worker_command and later erasing them if
    # needed.
    start_worker_command = [
        sys.executable,
        setup_worker_path,
        worker_path,
        f"--node-ip-address={node_ip_address}",
        "--node-manager-port=RAY_NODE_MANAGER_PORT_PLACEHOLDER",
        f"--object-store-name={plasma_store_name}",
        f"--raylet-name={raylet_name}",
        f"--redis-address={redis_address}",
        f"--storage={storage}",
        f"--temp-dir={temp_dir}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--gcs-address={gcs_address}",
        "RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER",
    ]

    if redis_password:
        start_worker_command += [f"--redis-password={redis_password}"]

    # If the object manager port is None, then use 0 to cause the object
    # manager to choose its own port.
    if object_manager_port is None:
        object_manager_port = 0

    if min_worker_port is None:
        min_worker_port = 0

    if max_worker_port is None:
        max_worker_port = 0

    agent_command = [
        sys.executable,
        "-u",
        os.path.join(RAY_PATH, "dashboard", "agent.py"),
        f"--node-ip-address={node_ip_address}",
        f"--metrics-export-port={metrics_export_port}",
        f"--dashboard-agent-port={metrics_agent_port}",
        f"--listen-port={dashboard_agent_listen_port}",
        "--node-manager-port=RAY_NODE_MANAGER_PORT_PLACEHOLDER",
        f"--object-store-name={plasma_store_name}",
        f"--raylet-name={raylet_name}",
        f"--temp-dir={temp_dir}",
        f"--session-dir={session_dir}",
        f"--runtime-env-dir={resource_dir}",
        f"--log-dir={log_dir}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--gcs-address={gcs_address}",
    ]
    if stdout_file is None and stderr_file is None:
        # If not redirecting logging to files, unset log filename.
        # This will cause log records to go to stderr.
        agent_command.append("--logging-filename=")
        # Use stderr log format with the component name as a message prefix.
        logging_format = ray_constants.LOGGER_FORMAT_STDERR.format(component=ray_constants.PROCESS_TYPE_DASHBOARD_AGENT)
        agent_command.append(f"--logging-format={logging_format}")

    if not ray._private.utils.check_dashboard_dependencies_installed():
        # If dependencies are not installed, it is the minimally packaged
        # ray. We should restrict the features within dashboard agent
        # that requires additional dependencies to be downloaded.
        agent_command.append("--minimal")

    command = [
        RAYLET_EXECUTABLE,
        f"--raylet_socket_name={raylet_name}",
        f"--store_socket_name={plasma_store_name}",
        f"--object_manager_port={object_manager_port}",
        f"--min_worker_port={min_worker_port}",
        f"--max_worker_port={max_worker_port}",
        f"--node_manager_port={node_manager_port}",
        f"--node_ip_address={node_ip_address}",
        f"--maximum_startup_concurrency={maximum_startup_concurrency}",
        f"--static_resource_list={resource_argument}",
        f"--python_worker_command={subprocess.list2cmdline(start_worker_command)}",    # noqa
    # f"--java_worker_command={subprocess.list2cmdline(java_worker_command)}",  # noqa
    # f"--cpp_worker_command={subprocess.list2cmdline(cpp_worker_command)}",  # noqa
        f"--native_library_path={DEFAULT_NATIVE_LIBRARY_PATH}",
        f"--redis_password={redis_password or ''}",
        f"--temp_dir={temp_dir}",
        f"--session_dir={session_dir}",
        f"--log_dir={log_dir}",
        f"--resource_dir={resource_dir}",
        f"--metrics-agent-port={metrics_agent_port}",
        f"--metrics_export_port={metrics_export_port}",
        f"--object_store_memory={object_store_memory}",
        f"--plasma_directory={plasma_directory}",
        f"--ray-debugger-external={1 if ray_debugger_external else 0}",
        f"--gcs-address={gcs_address}",
        f"--scheduler_address={scheduler_address}",
    ]

    if worker_port_list is not None:
        command.append(f"--worker_port_list={worker_port_list}")
    if start_initial_python_workers_for_first_job:
        command.append("--num_initial_python_workers_for_first_job={}".format(resource_spec.num_cpus))
    command.append("--agent_command={}".format(subprocess.list2cmdline(agent_command)))
    if huge_pages:
        command.append("--huge_pages")
    if socket_to_use:
        socket_to_use.close()
    if node_name is not None:
        command.append(f"--node-name={node_name}",)
    process_info = start_ray_process(
        command,
        ray_constants.PROCESS_TYPE_RAYLET,
        use_valgrind=use_valgrind,
        use_gdb=False,
        use_valgrind_profiler=use_profiler,
        use_perftools_profiler=("RAYLET_PERFTOOLS_PATH" in os.environ),
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=fate_share,
        env_updates=env_updates,
    )

    return process_info
