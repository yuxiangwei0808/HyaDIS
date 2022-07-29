#include "hyadis/csrc/scheduler/scheduler.h"

#include "ray/common/ray_config.h"

namespace ray {

namespace scheduling {

Scheduler::Scheduler(const SchedulerConfig &config,
                     instrumented_io_context &main_service,
                     std::shared_ptr<gcs::GcsClient> gcs_client)
    : config_(config),
      main_service_(main_service),
      rpc_server_(config.grpc_server_name, config.grpc_server_port,
                  config.node_ip_address == "127.0.0.1"),
      rpc_service_(main_service, *this),
      client_call_manager_(main_service),
      gcs_client_(gcs_client),
      periodical_runner_(main_service) {}

void Scheduler::Start() {
  periodical_runner_.RunFnPeriodically(
      [this]() { ScheduleAndDispatchTasks(); },
      RayConfig::instance().worker_cap_initial_backoff_delay_ms());

  rpc_server_.RegisterService(rpc_service_);
  rpc_server_.Run();

  RAY_LOG(INFO) << "HyaDIS scheduler is listening on "
                << config_.node_ip_address << ":" << config_.grpc_server_port
                << ".";
}

void Scheduler::Stop() {
  rpc_server_.Shutdown();
  RAY_LOG(INFO) << "HyaDIS scheduler is stopped.";
}

void Scheduler::HandleQueueAndScheduleTask(
    const rpc::scheduling::QueueAndScheduleTaskRequest &request,
    rpc::scheduling::QueueAndScheduleTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void Scheduler::HandleCancelTask(
    const rpc::scheduling::CancelTaskRequest &request,
    rpc::scheduling::CancelTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void Scheduler::HandleScheduleAndDispatchTasks(
    const rpc::scheduling::ScheduleAndDispatchTasksRequest &request,
    rpc::scheduling::ScheduleAndDispatchTasksReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

// void Scheduler::QueueAndScheduleTask(
//     const RayTask &task, bool grant_or_reject,
//     bool is_selected_based_on_locality, rpc::RequestWorkerLeaseReply *reply,
//     rpc::SendReplyCallback send_reply_callback) {}

// bool Scheduler::CancelTask(
//     const TaskID &task_id,
//     rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
//         rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
//     const std::string &scheduling_failure_message = "") {
//   return true;
// }

void Scheduler::ScheduleAndDispatchTasks() { return; }

}  // namespace scheduling
}  // namespace ray
