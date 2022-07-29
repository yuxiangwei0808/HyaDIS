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

#include "hyadis/csrc/raylet/raylet.h"

namespace ray {

namespace raylet {

HyadisRaylet::HyadisRaylet(
    instrumented_io_context &main_service, const std::string &socket_name,
    const std::string &node_ip_address, const std::string &node_name,
    const NodeManagerConfig &node_manager_config,
    const ObjectManagerConfig &object_manager_config,
    std::shared_ptr<gcs::GcsClient> gcs_client,
    std::shared_ptr<scheduling::SchedulerClient> scheduler_client,
    int metrics_export_port)
    : Raylet(main_service, socket_name, node_ip_address, node_name, gcs_client,
             metrics_export_port),
      scheduler_client_(scheduler_client) {
  node_manager_ = std::make_shared<HyadisNodeManager>(
      main_service, self_node_id_, node_name, node_manager_config,
      object_manager_config, gcs_client, scheduler_client_);
  self_node_info_.set_node_manager_address(node_ip_address);
  self_node_info_.set_object_store_socket_name(
      object_manager_config.store_socket_name);
  self_node_info_.set_object_manager_port(
      node_manager_->GetObjectManagerPort());
  self_node_info_.set_node_manager_port(node_manager_->GetServerPort());
  self_node_info_.set_node_manager_hostname(boost::asio::ip::host_name());
  auto resource_map = node_manager_config.resource_config.ToResourceMap();
  self_node_info_.mutable_resources_total()->insert(resource_map.begin(),
                                                    resource_map.end());
}

}  // namespace raylet
}  // namespace ray
