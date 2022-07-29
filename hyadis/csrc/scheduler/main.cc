#include <iostream>
#include <memory>
#include <string>

#include "gflags/gflags.h"
#include "hyadis/csrc/scheduler/scheduler.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"

DEFINE_string(redis_address, "", "The ip address of redis.");
DEFINE_int32(redis_port, -1, "The port of redis.");
DEFINE_string(log_dir, "", "The path of the dir where log files are created.");
DEFINE_string(gcs_address, "",
              "The address of the GCS server, including IP and port.");
DEFINE_string(node_ip_address, "", "The ip address of the node.");
DEFINE_int32(scheduler_port, 50051, "The port of scheduler.");
DEFINE_int32(metrics_agent_port, -1, "The port of metrics agent.");
DEFINE_string(redis_password, "", "The password of redis.");

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler(argv[0]);

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string gcs_address = FLAGS_gcs_address;
  const std::string node_ip_address = FLAGS_node_ip_address;
  const int scheduler_port = static_cast<int>(FLAGS_scheduler_port);
  const int metrics_agent_port = static_cast<int>(FLAGS_metrics_agent_port);
  const std::string log_dir = FLAGS_log_dir;

  // IO Service for main loop.
  instrumented_io_context main_service;
  // Ensure that the IO service keeps running. Without this, the main_service
  // will exit as soon as there is no more work to be processed.
  boost::asio::io_service::work work(main_service);

  // Initialize gcs client
  std::shared_ptr<ray::gcs::GcsClient> gcs_client;
  ray::gcs::GcsClientOptions client_options(gcs_address);
  gcs_client = std::make_shared<ray::gcs::GcsClient>(client_options);

  RAY_CHECK_OK(gcs_client->Connect(main_service));

  // Initialize scheduler
  ray::scheduling::SchedulerConfig scheduler_config;
  scheduler_config.grpc_server_name = "Scheduler";
  scheduler_config.grpc_server_port = scheduler_port;
  scheduler_config.grpc_server_thread_num = 1;
  scheduler_config.node_ip_address = node_ip_address;
  scheduler_config.log_dir = log_dir;

  ray::scheduling::Scheduler scheduler(scheduler_config, main_service,
                                       gcs_client);
  scheduler.Start();

  const ray::stats::TagsType global_tags = {
      {ray::stats::ComponentKey, "HyaDIS scheduler"},
      {ray::stats::VersionKey, ""},
      {ray::stats::NodeAddressKey, node_ip_address}};
  ray::stats::Init(global_tags, metrics_agent_port);

  auto handler = [&main_service, &scheduler, &gcs_client](
                     const boost::system::error_code &error,
                     int signal_number) {
    RAY_LOG(INFO) << "Scheduler received SIGTERM, shutting down...";
    scheduler.Stop();
    gcs_client->Disconnect();
    ray::stats::Shutdown();
    main_service.stop();
  };
  boost::asio::signal_set signals(main_service);
#ifdef _WIN32
  signals.add(SIGBREAK);
#else
  signals.add(SIGTERM);
#endif
  signals.async_wait(handler);

  main_service.run();

  return 0;
}
