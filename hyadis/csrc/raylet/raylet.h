// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <boost/asio.hpp>
#include <boost/asio/error.hpp>
#include <list>

#include "hyadis/csrc/raylet/node_manager.h"
#include "hyadis/csrc/scheduler/scheduler.h"
#include "hyadis/csrc/thirdparty/ray/raylet/raylet.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/object_manager/object_manager.h"

namespace ray {

namespace raylet {

class HyadisRaylet : public Raylet {
 public:
  /// Create a raylet server and listen for local clients.
  ///
  /// \param main_service The event loop to run the server on.
  /// \param object_manager_service The asio io_service tied to the object
  /// manager. \param socket_name The Unix domain socket to listen on for local
  /// clients. \param node_ip_address The IP address of this node. \param
  /// node_manager_config Configuration to initialize the node manager.
  /// scheduler with.
  /// \param object_manager_config Configuration to initialize the object
  /// manager.
  /// \param gcs_client A client connection to the GCS.
  /// \param scheduler_client A client connection to the scheduler.
  /// \param metrics_export_port A port at which metrics are exposed to.
  HyadisRaylet(instrumented_io_context &main_service,
               const std::string &socket_name,
               const std::string &node_ip_address, const std::string &node_name,
               const NodeManagerConfig &node_manager_config,
               const ObjectManagerConfig &object_manager_config,
               std::shared_ptr<gcs::GcsClient> gcs_client,
               std::shared_ptr<scheduling::SchedulerClient> scheduler_client,
               int metrics_export_port);

 private:
  /// A client connection to the scheduler.
  std::shared_ptr<scheduling::SchedulerClient> scheduler_client_;
};

}  // namespace raylet
}  // namespace ray
