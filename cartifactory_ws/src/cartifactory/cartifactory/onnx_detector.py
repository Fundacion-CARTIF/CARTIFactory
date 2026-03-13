#!/usr/bin/env python3
import math
import os
from typing import Optional, Tuple, List

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    qos_profile_sensor_data,
    QoSProfile,
    DurabilityPolicy,
    ReliabilityPolicy,
    HistoryPolicy,
)

from sensor_msgs.msg import Image
from vision_msgs.msg import Detection2DArray, Detection2D, ObjectHypothesisWithPose
from vision_msgs.msg import BoundingBox2D

from cv_bridge import CvBridge
import json
from std_msgs.msg import String

from custom_interfaces.msg import PipelineStats

import cv2
import numpy as np

try:
    import tomllib
except ModuleNotFoundError:
    import toml as tomllib

import onnxruntime as ort


def _make_qos_from_mode(mode: str, depth: int = 10) -> QoSProfile:
    m = (mode or "").strip().lower()
    qos = QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=depth,
        durability=DurabilityPolicy.VOLATILE,
        reliability=ReliabilityPolicy.BEST_EFFORT,
    )
    if m == "reliable":
        qos.reliability = ReliabilityPolicy.RELIABLE
    return qos


def _wrap_to_half_pi(theta: float) -> float:
    theta = (theta + math.pi) % (2.0 * math.pi) - math.pi
    if theta >= math.pi / 2:
        theta -= math.pi
    if theta < -math.pi / 2:
        theta += math.pi
    return theta


def _obb_from_mask(mask_bin: np.ndarray) -> Optional[Tuple[float, float, float, float, float]]:
    cnts, _ = cv2.findContours(mask_bin, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not cnts:
        return None
    c = max(cnts, key=cv2.contourArea)
    if cv2.contourArea(c) < 10:
        return None

    rect = cv2.minAreaRect(c)
    (cx, cy), (w, h), angle_deg = rect

    if w < h:
        w, h = h, w
        angle_deg += 90.0

    theta_rad = math.radians(angle_deg)
    theta_rad = _wrap_to_half_pi(theta_rad)
    return float(cx), float(cy), float(w), float(h), float(theta_rad)


def _xywh2xyxy(xywh: np.ndarray) -> np.ndarray:
    out = np.zeros_like(xywh, dtype=np.float32)
    out[:, 0] = xywh[:, 0] - xywh[:, 2] / 2.0
    out[:, 1] = xywh[:, 1] - xywh[:, 3] / 2.0
    out[:, 2] = xywh[:, 0] + xywh[:, 2] / 2.0
    out[:, 3] = xywh[:, 1] + xywh[:, 3] / 2.0
    return out


def _nms_xyxy(boxes: np.ndarray, scores: np.ndarray, iou_thres: float = 0.45) -> List[int]:
    if boxes.shape[0] == 0:
        return []

    x1 = boxes[:, 0].astype(np.float32)
    y1 = boxes[:, 1].astype(np.float32)
    x2 = boxes[:, 2].astype(np.float32)
    y2 = boxes[:, 3].astype(np.float32)

    areas = np.clip(x2 - x1, 0, None) * np.clip(y2 - y1, 0, None)
    order = scores.argsort()[::-1]

    keep: List[int] = []
    while order.size > 0:
        i = int(order[0])
        keep.append(i)
        if order.size == 1:
            break

        xx1 = np.maximum(x1[i], x1[order[1:]])
        yy1 = np.maximum(y1[i], y1[order[1:]])
        xx2 = np.minimum(x2[i], x2[order[1:]])
        yy2 = np.minimum(y2[i], y2[order[1:]])

        w = np.clip(xx2 - xx1, 0, None)
        h = np.clip(yy2 - yy1, 0, None)
        inter = w * h
        iou = inter / (areas[i] + areas[order[1:]] - inter + 1e-9)

        inds = np.where(iou <= iou_thres)[0]
        order = order[inds + 1]

    return keep


class DetectorOnnxNode(Node):
    def __init__(self):
        super().__init__('detector_onnx')

        self.declare_parameter('image_topic', '/camera/camera/color/image_raw')
        self.declare_parameter('detections_topic', '/detections')

        self.declare_parameter('detections_qos', 'sensor_data')
        self.declare_parameter('debug_qos', 'best_effort')

        self.declare_parameter('toml_path', '')
        self.declare_parameter('model_path', '')
        self.declare_parameter('device', 'cuda')

        self.declare_parameter('conf', -1.0)
        self.declare_parameter('iou', 0.45)
        self.declare_parameter('img_h', 480)
        self.declare_parameter('img_w', 640)

        self.declare_parameter('use_mask_obb', True)
        self.declare_parameter('publish_debug_image', True)
        self.declare_parameter('class_id_mode', 'name')

        # stats
        self.declare_parameter('publish_stats', True)
        self.declare_parameter('stats_topic', '/stats/detector')
        self.declare_parameter('drop_if_busy', False)

        self.image_topic = self.get_parameter('image_topic').value
        self.detections_topic = self.get_parameter('detections_topic').value

        self.detections_qos_mode = self.get_parameter('detections_qos').value
        self.debug_qos_mode = self.get_parameter('debug_qos').value

        self.toml_path = self.get_parameter('toml_path').value
        self.model_path_param = self.get_parameter('model_path').value
        self.device = self.get_parameter('device').value

        self.conf_param = float(self.get_parameter('conf').value)
        self.iou = float(self.get_parameter('iou').value)

        self.img_h = int(self.get_parameter('img_h').value)
        self.img_w = int(self.get_parameter('img_w').value)

        self.use_mask_obb = bool(self.get_parameter('use_mask_obb').value)
        self.publish_debug_image = bool(self.get_parameter('publish_debug_image').value)

        self.class_id_mode = str(self.get_parameter('class_id_mode').value).strip().lower()
        if self.class_id_mode not in ("id", "name"):
            self.get_logger().warning(f'Invalid class_id_mode "{self.class_id_mode}". Falling back to "id".')
            self.class_id_mode = "id"

        self.publish_stats = bool(self.get_parameter('publish_stats').value)
        self.stats_topic = self.get_parameter('stats_topic').value
        self.drop_if_busy = bool(self.get_parameter('drop_if_busy').value)

        self.class_map_topic = f"{self.detections_topic}/class_map"
        self.debug_image_topic = f"{self.detections_topic}/image"

        qos_latched = QoSProfile(depth=1)
        qos_latched.durability = DurabilityPolicy.TRANSIENT_LOCAL
        qos_latched.reliability = ReliabilityPolicy.RELIABLE
        self.pub_class_map = self.create_publisher(String, self.class_map_topic, qos_latched)

        det_qos = _make_qos_from_mode(self.detections_qos_mode, depth=10)
        dbg_qos = _make_qos_from_mode(self.debug_qos_mode, depth=10)

        if self.publish_debug_image:
            self.pub_debug_img = self.create_publisher(Image, self.debug_image_topic, dbg_qos)

        self.model_type: str = "Unknown"
        self.class_names: Optional[List[str]] = None
        toml_conf_default: Optional[float] = None
        cfg = None
        toml_dir: Optional[str] = None

        if self.toml_path:
            try:
                if tomllib.__name__ == "tomllib":
                    with open(self.toml_path, "rb") as f:
                        cfg = tomllib.load(f)
                else:
                    cfg = tomllib.load(self.toml_path)

                toml_dir = os.path.dirname(os.path.abspath(self.toml_path))
                self.model_type = str(cfg.get("parameters", {}).get("type", "Unknown"))
                self.class_names = cfg.get("classes", {}).get("classes", None)
                toml_conf_default = cfg.get("parameters", {}).get("confidence", None)

                self.get_logger().info(
                    f'Loaded TOML: type={self.model_type}, '
                    f'classes={len(self.class_names) if self.class_names else "n/a"}, '
                    f'confidence={toml_conf_default if toml_conf_default is not None else "n/a"}'
                )
            except Exception as e:
                self.get_logger().warning(f'Failed to load TOML "{self.toml_path}": {e}')
                cfg = None
                toml_dir = None

        if self.conf_param > 0.0:
            self.conf = self.conf_param
        elif toml_conf_default is not None:
            self.conf = float(toml_conf_default)
        else:
            self.conf = 0.25

        model_path: str = ""
        if self.model_path_param:
            model_path = self.model_path_param
        elif cfg is not None:
            weights_in_toml = cfg.get("model", {}).get("weights", "")
            if weights_in_toml:
                model_path = weights_in_toml

        if not model_path:
            raise RuntimeError(
                'No model weights specified. Provide ROS param "model_path" OR add [model].weights in TOML and pass "toml_path".'
            )

        if not os.path.isabs(model_path) and toml_dir is not None:
            model_path = os.path.join(toml_dir, model_path)

        self.model_path = model_path
        self.get_logger().info(f'Final ONNX path: {self.model_path}')

        self.get_logger().info(f'Loading ONNX model with onnxruntime: {self.model_path}')
        self.sess = self._make_ort_session(self.model_path, self.device)

        self.inp = self.sess.get_inputs()[0]
        self.in_name = self.inp.name
        self.in_shape = self.inp.shape

        try:
            if isinstance(self.in_shape, (list, tuple)) and len(self.in_shape) == 4:
                if isinstance(self.in_shape[2], int) and isinstance(self.in_shape[3], int):
                    self.img_h = int(self.in_shape[2])
                    self.img_w = int(self.in_shape[3])
        except Exception:
            pass

        self._published_class_map = False

        self.bridge = CvBridge()

        self.sub_img = self.create_subscription(
            Image,
            self.image_topic,
            self.on_image,
            qos_profile_sensor_data
        )

        self.pub_det = self.create_publisher(Detection2DArray, self.detections_topic, det_qos)

        if self.publish_stats:
            stats_qos = QoSProfile(
                history=HistoryPolicy.KEEP_LAST,
                depth=10,
                durability=DurabilityPolicy.VOLATILE,
                reliability=ReliabilityPolicy.RELIABLE,
            )
            self.pub_stats = self.create_publisher(PipelineStats, self.stats_topic, stats_qos)

        self._busy = False
        self._dropped_frames = 0
        self._last_num_dets = 0

        self._last_published_dropped_frames = 0

        self.get_logger().info(
            f'(ONNX) detector ready. image_topic="{self.image_topic}", detections_topic="{self.detections_topic}", '
            f'conf={self.conf}, iou={self.iou}, input_hw=({self.img_h},{self.img_w}), use_mask_obb={self.use_mask_obb}, '
            f'class_id_mode={self.class_id_mode}, class_map_topic="{self.class_map_topic}", '
            f'debug_image_topic="{self.debug_image_topic}", stats_topic="{self.stats_topic}"'
        )

        if self.publish_stats:
            self._publish_stats_event()

    @staticmethod
    def _axis_aligned_bbox(xmin: float, ymin: float, xmax: float, ymax: float) -> Tuple[float, float, float, float]:
        cx = (xmin + xmax) / 2.0
        cy = (ymin + ymax) / 2.0
        w = (xmax - xmin)
        h = (ymax - ymin)
        return float(cx), float(cy), float(w), float(h)

    def _publish_stats_event(self):
        if not self.publish_stats:
            return

        msg = PipelineStats()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.node_name = self.get_name()

        msg.frames_dropped = int(self._dropped_frames)

        msg.action_goals_received = 0
        msg.action_goals_accepted = 0
        msg.action_goals_rejected = 0
        msg.action_goals_canceled = 0
        msg.action_goals_succeeded = 0
        msg.action_goals_failed = 0

        msg.match_requests = 0
        msg.match_success = 0
        msg.match_fail = 0

        msg.fps_input = 0.0
        msg.fps_processed = 0.0
        msg.avg_inference_ms = 0.0

        msg.camera_available = True

        self.pub_stats.publish(msg)

    def _maybe_publish_stats_event(self):
        if not self.publish_stats:
            return

        if self._dropped_frames > self._last_published_dropped_frames:
            self._publish_stats_event()
            self._last_published_dropped_frames = self._dropped_frames

    def _publish_class_map_if_needed(self, nc: int):
        if self._published_class_map:
            return

        if not self.class_names or len(self.class_names) < nc:
            self.class_names = [str(i) for i in range(nc)]
            if self.class_id_mode == "name":
                self.get_logger().warning(
                    "class_id_mode=name pero no hay class names reales en TOML -> usando nombres '0..nc-1'."
                )

        payload = {str(i): str(name) for i, name in enumerate(self.class_names)}
        m = String()
        m.data = json.dumps(payload, ensure_ascii=False)
        self.pub_class_map.publish(m)
        self.get_logger().info(f'Published class map on "{self.class_map_topic}" ({len(self.class_names)} classes)')
        self._published_class_map = True

    def _make_ort_session(self, model_path: str, device_param: str) -> ort.InferenceSession:
        requested = str(device_param).strip().lower()

        available = ort.get_available_providers()
        self.get_logger().info(f"ONNXRuntime available providers: {available}")

        use_cuda = False
        device_id = 0

        if requested == "cpu":
            use_cuda = False
        elif requested.startswith("cuda"):
            use_cuda = True
            if ":" in requested:
                try:
                    device_id = int(requested.split(":")[1])
                except Exception:
                    device_id = 0
        elif requested.isdigit():
            use_cuda = True
            device_id = int(requested)
        else:
            self.get_logger().warning(f'Unknown device "{device_param}". Falling back to CPU.')
            use_cuda = False

        if use_cuda and "CUDAExecutionProvider" in available:
            providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
            provider_options = [{"device_id": device_id}, {}]
            self.get_logger().info(f"Using ONNXRuntime CUDAExecutionProvider (device_id={device_id})")
        else:
            if use_cuda and "CUDAExecutionProvider" not in available:
                self.get_logger().warning(
                    "CUDA requested but CUDAExecutionProvider is NOT available. Falling back to CPU."
                )
            providers = ["CPUExecutionProvider"]
            provider_options = [{}]
            self.get_logger().info("Using ONNXRuntime CPUExecutionProvider")

        so = ort.SessionOptions()
        so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL

        return ort.InferenceSession(
            model_path,
            sess_options=so,
            providers=providers,
            provider_options=provider_options,
        )

    def _preprocess(self, frame_bgr: np.ndarray) -> Tuple[np.ndarray, float, float, np.ndarray]:
        h0, w0 = frame_bgr.shape[:2]
        H, W = self.img_h, self.img_w

        frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
        frame_rgb_rs = cv2.resize(frame_rgb, (W, H), interpolation=cv2.INTER_LINEAR)

        x = frame_rgb_rs.astype(np.float32) / 255.0
        x = np.transpose(x, (2, 0, 1))
        x = np.expand_dims(x, 0)
        x = np.ascontiguousarray(x)

        sx = w0 / float(W)
        sy = h0 / float(H)
        return x, sx, sy, frame_rgb_rs

    def on_image(self, msg: Image):
        if self.drop_if_busy and self._busy:
            self._dropped_frames += 1
            self._maybe_publish_stats_event()
            return

        self._busy = True
        try:
            try:
                frame_bgr = self.bridge.imgmsg_to_cv2(msg, desired_encoding='bgr8')
            except Exception as e:
                self.get_logger().error(f'cv_bridge conversion failed: {e}')
                return

            x, sx, sy, _ = self._preprocess(frame_bgr)

            outputs = self.sess.run(None, {self.in_name: x})

            det_array = Detection2DArray()
            det_array.header = msg.header

            if len(outputs) < 2:
                self._last_num_dets = 0
                self.pub_det.publish(det_array)

                if self.publish_debug_image:
                    debug_msg = self.bridge.cv2_to_imgmsg(frame_bgr, encoding="bgr8")
                    debug_msg.header = msg.header
                    self.pub_debug_img.publish(debug_msg)
                return

            out0 = outputs[0]
            proto = outputs[1]

            if out0.ndim != 3:
                self.get_logger().warning(f"Unexpected out0 shape: {out0.shape}")
                self._last_num_dets = 0
                self.pub_det.publish(det_array)
                return

            out0 = out0[0]
            if out0.shape[0] < out0.shape[1]:
                out0 = out0.T

            if proto.ndim != 4 or proto.shape[0] != 1:
                self.get_logger().warning(f"Unexpected proto shape: {proto.shape}")
                self._last_num_dets = 0
                self.pub_det.publish(det_array)
                return

            proto = proto[0]
            nm, mh, mw = proto.shape

            _, C = out0.shape
            nc = C - 4 - nm
            if nc <= 0:
                self.get_logger().warning(f"Cannot infer nc: C={C}, nm={nm} => nc={nc}")
                self._last_num_dets = 0
                self.pub_det.publish(det_array)
                return

            self._publish_class_map_if_needed(nc)

            boxes_xywh = out0[:, 0:4].astype(np.float32)
            cls_scores = out0[:, 4:4 + nc].astype(np.float32)
            mask_coefs = out0[:, 4 + nc:].astype(np.float32)

            cls_ids = np.argmax(cls_scores, axis=1)
            scores = np.max(cls_scores, axis=1)

            keep = scores >= float(self.conf)
            boxes_xywh = boxes_xywh[keep]
            scores = scores[keep]
            cls_ids = cls_ids[keep]
            mask_coefs = mask_coefs[keep]

            if boxes_xywh.shape[0] == 0:
                self._last_num_dets = 0
                self.pub_det.publish(det_array)

                if self.publish_debug_image:
                    debug_msg = self.bridge.cv2_to_imgmsg(frame_bgr, encoding="bgr8")
                    debug_msg.header = msg.header
                    self.pub_debug_img.publish(debug_msg)
                return

            boxes_xyxy = _xywh2xyxy(boxes_xywh)
            keep_idx = _nms_xyxy(boxes_xyxy, scores, iou_thres=float(self.iou))
            boxes_xyxy = boxes_xyxy[keep_idx]
            scores = scores[keep_idx]
            cls_ids = cls_ids[keep_idx]
            mask_coefs = mask_coefs[keep_idx]

            proto_flat = proto.reshape(nm, -1)
            masks_flat = mask_coefs @ proto_flat
            masks = 1.0 / (1.0 + np.exp(-masks_flat.clip(-50, 50)))
            masks = masks.reshape(-1, mh, mw)

            H, W = self.img_h, self.img_w
            masks_up = []
            for m in masks:
                masks_up.append(cv2.resize(m, (W, H), interpolation=cv2.INTER_LINEAR))
            masks_up = np.stack(masks_up, axis=0)

            debug_frame = frame_bgr.copy() if self.publish_debug_image else None
            h0, w0 = frame_bgr.shape[:2]

            for i in range(len(scores)):
                x1, y1, x2, y2 = boxes_xyxy[i]
                x1o = float(x1) * sx
                x2o = float(x2) * sx
                y1o = float(y1) * sy
                y2o = float(y2) * sy

                cx, cy, w, h = self._axis_aligned_bbox(x1o, y1o, x2o, y2o)
                theta = 0.0

                mask_bin_rs = (masks_up[i] > 0.5).astype(np.uint8)
                mask_bin = cv2.resize(mask_bin_rs, (w0, h0), interpolation=cv2.INTER_NEAREST)

                # Recortar máscara al bounding box axis-aligned
                x1i = int(np.clip(np.floor(x1o), 0, w0 - 1))
                y1i = int(np.clip(np.floor(y1o), 0, h0 - 1))
                x2i = int(np.clip(np.ceil(x2o), 0, w0 - 1))
                y2i = int(np.clip(np.ceil(y2o), 0, h0 - 1))

                mask_bbox = np.zeros_like(mask_bin, dtype=np.uint8)
                if x2i > x1i and y2i > y1i:
                    mask_bbox[y1i:y2i + 1, x1i:x2i + 1] = 1

                mask_bin = (mask_bin * mask_bbox).astype(np.uint8)

                if self.use_mask_obb:
                    obb = _obb_from_mask(mask_bin)
                    if obb is not None:
                        cx, cy, w, h, theta = obb

                d = Detection2D()
                bbox = BoundingBox2D()
                bbox.center.position.x = float(cx)
                bbox.center.position.y = float(cy)
                bbox.center.theta = float(theta)
                bbox.size_x = float(w)
                bbox.size_y = float(h)
                d.bbox = bbox

                hyp = ObjectHypothesisWithPose()
                cid = int(cls_ids[i])

                if self.class_id_mode == "name" and self.class_names and 0 <= cid < len(self.class_names):
                    class_label = str(self.class_names[cid])
                else:
                    class_label = str(cid)

                hyp.hypothesis.class_id = class_label
                hyp.hypothesis.score = float(scores[i])
                hyp.pose.pose.position.x = float(cx)
                hyp.pose.pose.position.y = float(cy)
                hyp.pose.pose.position.z = 0.0

                d.results.append(hyp)
                det_array.detections.append(d)

                if debug_frame is not None:
                    green = np.array([0, 255, 0], dtype=np.uint8)
                    m = mask_bin.astype(bool)
                    debug_frame[m] = (0.6 * debug_frame[m] + 0.4 * green).astype(np.uint8)

                    cv2.rectangle(debug_frame, (x1i, y1i), (x2i, y2i), (0, 0, 255), 2)

                    if self.use_mask_obb:
                        obb = _obb_from_mask(mask_bin)
                        if obb is not None:
                            ocx, ocy, ow, oh, otheta = obb
                            rect = ((ocx, ocy), (ow, oh), math.degrees(otheta))
                            pts = cv2.boxPoints(rect).astype(np.int32)
                            cv2.polylines(debug_frame, [pts], True, (255, 0, 0), 2)

                    cv2.putText(
                        debug_frame,
                        f"{class_label} {float(scores[i]):.2f}",
                        (x1i, max(0, y1i - 5)),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.7,
                        (255, 255, 255),
                        2,
                        cv2.LINE_AA,
                    )

            self._last_num_dets = len(det_array.detections)

            self.pub_det.publish(det_array)

            if self.publish_debug_image and debug_frame is not None:
                debug_msg = self.bridge.cv2_to_imgmsg(debug_frame, encoding="bgr8")
                debug_msg.header = msg.header
                self.pub_debug_img.publish(debug_msg)

        finally:
            self._busy = False


def main():
    rclpy.init()
    node = DetectorOnnxNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()