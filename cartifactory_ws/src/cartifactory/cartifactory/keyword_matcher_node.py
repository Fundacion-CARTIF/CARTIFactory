#!/usr/bin/env python3
import json
import time
from typing import Dict, Any, List

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    ReliabilityPolicy,
    DurabilityPolicy,
    HistoryPolicy,
    qos_profile_sensor_data,
)
from rclpy.action import ActionServer, CancelResponse, GoalResponse

from std_msgs.msg import String, Bool
from sensor_msgs.msg import Image, CameraInfo
from vision_msgs.msg import Detection2DArray

from custom_interfaces.action import MatchAction
from custom_interfaces.msg import PipelineStats


def _norm(s: str, case_insensitive: bool) -> str:
    s = (s or "").strip()
    return s.lower() if case_insensitive else s


class KeywordMatcherNode(Node):
    """
    Sub:
      - /detections (vision_msgs/Detection2DArray)
      - /detections/class_map (std_msgs/String JSON) [optional]
      - /detections/image (sensor_msgs/Image)
      - /camera/.../camera_info (sensor_msgs/CameraInfo)

    Pub:
      - /keyword_match (std_msgs/Bool)
      - /keyword_matches (std_msgs/String JSON)
      - /detections_matched (vision_msgs/Detection2DArray) [optional]
      - /stats/matcher (custom_interfaces/PipelineStats) [event-driven]
    """

    def __init__(self):
        super().__init__("keyword_matcher")

        # ---- Params ----
        self.declare_parameter("detections_topic", "/detections")
        self.declare_parameter("class_map_topic", "/detections/class_map")
        self.declare_parameter("keyword_topic", "/keyword")
        self.declare_parameter("image_topic", "/camera/camera/color/image_raw")
        self.declare_parameter("camera_info_topic", "/camera/camera/color/camera_info")

        self.declare_parameter("publish_matched_detections", True)
        self.declare_parameter("detections_matched_topic", "/detections_matched")

        self.declare_parameter("match_any", True)
        self.declare_parameter("case_insensitive", True)
        self.declare_parameter("score_threshold", 0.0)
        self.declare_parameter("class_id_mode", "name")  # "name" | "id" | "auto"

        # stats
        self.declare_parameter("publish_stats", True)
        self.declare_parameter("stats_topic", "/stats/matcher")

        self.detections_topic = self.get_parameter("detections_topic").value
        self.class_map_topic = self.get_parameter("class_map_topic").value
        self.keyword_topic = self.get_parameter("keyword_topic").value
        self.camera_topic = self.get_parameter("image_topic").value
        self.camera_info_topic = self.get_parameter("camera_info_topic").value
        self.debug_image_topic = f"{self.detections_topic}/image"

        self.publish_matched_detections = bool(self.get_parameter("publish_matched_detections").value)
        self.detections_matched_topic = self.get_parameter("detections_matched_topic").value

        self.match_any = bool(self.get_parameter("match_any").value)
        self.case_insensitive = bool(self.get_parameter("case_insensitive").value)
        self.score_threshold = float(self.get_parameter("score_threshold").value)
        self.class_id_mode = str(self.get_parameter("class_id_mode").value).strip().lower()

        self.publish_stats = bool(self.get_parameter("publish_stats").value)
        self.stats_topic = self.get_parameter("stats_topic").value

        if self.class_id_mode not in ("name", "id", "auto"):
            self.get_logger().warning(f'class_id_mode="{self.class_id_mode}" inválido. Usando "auto".')
            self.class_id_mode = "auto"

        # ---- State ----
        self._class_map: Dict[str, str] = {}
        self._keyword: str = ""

        # event counters
        self._goals_received = 0
        self._goals_accepted = 0
        self._goals_rejected = 0
        self._goals_canceled = 0
        self._goals_succeeded = 0
        self._goals_failed = 0

        self._match_requests = 0
        self._match_success = 0
        self._match_fail = 0

        self._last_camera_available_published = None

        # ---- QoS ----
        qos_det = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )

        qos_latched = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
        )

        qos_kw = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
        )
        _ = qos_kw  # por ahora no se usa

        # ---- Subs ----
        self.sub_det = self.create_subscription(
            Detection2DArray, self.detections_topic, self.on_detections, qos_det
        )
        self.sub_map = self.create_subscription(
            String, self.class_map_topic, self.on_class_map, qos_latched
        )
        self.sub_image = self.create_subscription(
            Image, self.debug_image_topic, self.on_image, qos_profile_sensor_data
        )
        self.sub_camera_info = self.create_subscription(
            CameraInfo, self.camera_info_topic, self.on_camera_info, 10
        )

        # ---- Pubs ----
        self.pub_match = self.create_publisher(Bool, "/keyword_match", 10)
        self.pub_matches = self.create_publisher(String, "/keyword_matches", 10)

        if self.publish_matched_detections:
            self.pub_det_matched = self.create_publisher(
                Detection2DArray, self.detections_matched_topic, 10
            )
        else:
            self.pub_det_matched = None

        if self.publish_stats:
            stats_qos = QoSProfile(
                history=HistoryPolicy.KEEP_LAST,
                depth=10,
                reliability=ReliabilityPolicy.RELIABLE,
                durability=DurabilityPolicy.VOLATILE,
            )
            self.pub_stats = self.create_publisher(PipelineStats, self.stats_topic, stats_qos)

        # ---- Actions ----
        self.match_action_server = ActionServer(
            node=self,
            action_type=MatchAction,
            action_name="/detection/match",
            execute_callback=self.match_execute_callback,
            goal_callback=self.match_goal_callback,
            cancel_callback=self.match_cancel_callback,
        )

        self.get_logger().info(
            f"Keyword matcher ready.\n"
            f"  Sub: {self.detections_topic}, {self.class_map_topic}, {self.keyword_topic}\n"
            f"  Pub: /keyword_match, /keyword_matches"
            f"{' , ' + self.detections_matched_topic if self.publish_matched_detections else ''}\n"
            f"  Stats: {self.stats_topic if self.publish_stats else 'disabled'}\n"
            f"  class_id_mode={self.class_id_mode}, score_threshold={self.score_threshold}, case_insensitive={self.case_insensitive}"
        )

        # camera monitoring
        self.timeout_sec = 5
        self.last_msg_time = self.get_clock().now()
        self.timer = self.create_timer(1.0, self.check_camera_timeout)
        self.camera_available = False

        self._latest_frame = Image()
        self._last_detections = Detection2DArray()

    def _publish_stats_event(self):
        if not self.publish_stats:
            return

        msg = PipelineStats()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.node_name = self.get_name()

        # detector side not used here
        msg.frames_dropped = 0

        # action/match side
        msg.action_goals_received = int(self._goals_received)
        msg.action_goals_accepted = int(self._goals_accepted)
        msg.action_goals_rejected = int(self._goals_rejected)
        msg.action_goals_canceled = int(self._goals_canceled)
        msg.action_goals_succeeded = int(self._goals_succeeded)
        msg.action_goals_failed = int(self._goals_failed)

        msg.match_requests = int(self._match_requests)
        msg.match_success = int(self._match_success)
        msg.match_fail = int(self._match_fail)

        # matcher metrics
        msg.fps_input = 0.0
        msg.fps_processed = 0.0
        msg.avg_inference_ms = 0.0

        msg.camera_available = bool(self.camera_available)

        self.pub_stats.publish(msg)

    def check_camera_timeout(self) -> None:
        now = self.get_clock().now()
        elapsed = (now - self.last_msg_time).nanoseconds / 1e9

        prev = self.camera_available

        if elapsed > self.timeout_sec:
            self.camera_available = False
        else:
            self.camera_available = True

        if self._last_camera_available_published is None:
            self._last_camera_available_published = self.camera_available
        elif self.camera_available != self._last_camera_available_published:
            self._publish_stats_event()
            self._last_camera_available_published = self.camera_available

    def on_camera_info(self, msg: CameraInfo):
        _ = msg
        self.last_msg_time = self.get_clock().now()

    def on_image(self, msg: Image):
        self._latest_frame = msg.data or Image()

    def on_class_map(self, msg: String):
        try:
            data = json.loads(msg.data)
            if isinstance(data, dict):
                self._class_map = {str(k): str(v) for k, v in data.items()}
                self.get_logger().info(f"Recibido class_map con {len(self._class_map)} entradas")
            else:
                self.get_logger().warning("class_map JSON no es un dict")
        except Exception as e:
            self.get_logger().warning(f"Error parseando class_map JSON: {e}")

    def _resolve_label(self, class_id_field: str) -> str:
        cid = (class_id_field or "").strip()

        if self.class_id_mode == "name":
            return cid

        if self.class_id_mode == "id":
            return self._class_map.get(cid, cid)

        if cid.isdigit():
            return self._class_map.get(cid, cid)
        return cid

    def on_detections(self, msg: Detection2DArray):
        self._last_detections = msg

    def process_detections(self, keyword: str, detection: Detection2DArray, feedback_pub):
        self.get_logger().info(f"Starting matching for: {keyword}")

        kw = _norm(keyword, self.case_insensitive)

        if not kw:
            feedback_pub("No keyword detected")
            self.pub_match.publish(Bool(data=False))

            out = {"keyword": "", "matches": [], "match": False}
            self.pub_matches.publish(String(data=json.dumps(out, ensure_ascii=False)))

            empty = Detection2DArray()
            empty.header = detection.header
            return empty

        matches: List[Dict[str, Any]] = []
        matched_dets: List = []

        for det in detection.detections:
            if not det.results:
                continue

            hyp0 = det.results[0].hypothesis
            class_id_field = hyp0.class_id
            score = float(hyp0.score)

            if score < self.score_threshold:
                continue

            label = self._resolve_label(class_id_field)
            label_n = _norm(label, self.case_insensitive)

            if label_n == kw:
                matches.append(
                    {
                        "class_id_field": class_id_field,
                        "label": label,
                        "score": score,
                        "cx": float(det.bbox.center.position.x),
                        "cy": float(det.bbox.center.position.y),
                        "w": float(det.bbox.size_x),
                        "h": float(det.bbox.size_y),
                        "theta": float(det.bbox.center.theta),
                    }
                )
                matched_dets.append(det)

                if self.match_any:
                    break

        is_match = len(matches) > 0

        self.pub_match.publish(Bool(data=is_match))

        out = {"keyword": self._keyword, "match": is_match, "matches": matches}
        self.pub_matches.publish(String(data=json.dumps(out, ensure_ascii=False)))
        feedback_pub("JSON details published")

        da = Detection2DArray()
        da.header = detection.header
        da.detections = matched_dets
        return da

    def match_goal_callback(self, goal_request: MatchAction.Goal) -> GoalResponse:
        self._goals_received += 1
        self._keyword = goal_request.kw

        try:
            if self.camera_available and isinstance(self._keyword, str):
                self._goals_accepted += 1
                self._publish_stats_event()
                return GoalResponse.ACCEPT
            else:
                self._goals_rejected += 1
                self.get_logger().warn("Action not started. No camera connected.")
                self._publish_stats_event()
                return GoalResponse.REJECT

        except Exception as e:
            self._goals_rejected += 1
            self.get_logger().info(f"Problem trying to match: {e}")
            self._publish_stats_event()
            return GoalResponse.REJECT

    def match_cancel_callback(self, goal_handle) -> CancelResponse:
        _ = goal_handle
        self.get_logger().info("Received cancel request in action server")
        self._goals_canceled += 1
        self._publish_stats_event()
        return CancelResponse.ACCEPT

    async def match_execute_callback(self, goal_handle) -> MatchAction.Result:
        feedback_msg = MatchAction.Feedback()
        result_msg = MatchAction.Result()

        def fb(text: str) -> None:
            goal_handle.publish_feedback(MatchAction.Feedback(feedback=text))

        t0 = time.perf_counter()
        self._match_requests += 1
        self._publish_stats_event()

        feedback_msg.feedback = "Starting matching process"
        goal_handle.publish_feedback(feedback_msg)
        self.get_logger().info("Starting matching process")

        try:
            det = self.process_detections(
                keyword=self._keyword,
                detection=self._last_detections,
                feedback_pub=fb
            )

            if len(det.detections) == 0:
                feedback_msg.feedback = "No matches found"
                result_msg.match_success = False
                result_msg.det = Detection2DArray()

                self._match_fail += 1
            else:
                feedback_msg.feedback = "Matching process succesfull"
                result_msg.match_success = True
                result_msg.det = det

                self._match_success += 1

                if self.pub_det_matched is not None:
                    self.pub_det_matched.publish(det)

            goal_handle.publish_feedback(feedback_msg)

            result_msg.action_success = True
            result_msg.img = Image()

            self._goals_succeeded += 1
            goal_handle.succeed()

        except Exception as e:
            feedback_msg.feedback = f"There has been a problem in the matching process:\n{e}"
            self.get_logger().warn(f"There has been a problem in the matching process:\n{e}")

            goal_handle.publish_feedback(feedback_msg)

            result_msg.action_success = False
            result_msg.match_success = False
            result_msg.det = Detection2DArray()
            result_msg.img = self._latest_frame

            self._goals_failed += 1
            self._match_fail += 1
            goal_handle.abort()

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        _ = elapsed_ms  # por ahora no se publica; se puede añadir luego si quieres

        self._publish_stats_event()
        return result_msg


def main():
    rclpy.init()
    node = KeywordMatcherNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()