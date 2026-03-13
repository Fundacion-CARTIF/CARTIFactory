#!/usr/bin/env python3
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():

    toml_path = LaunchConfiguration("toml_path")
    image_topic = LaunchConfiguration("image_topic")
    detections_topic = LaunchConfiguration("detections_topic")
    publish_debug_image = LaunchConfiguration("publish_debug_image")
    detections_qos = LaunchConfiguration("detections_qos")
    debug_qos = LaunchConfiguration("debug_qos")
    img_h = LaunchConfiguration("img_h")
    img_w = LaunchConfiguration("img_w")
    class_id_mode = LaunchConfiguration("class_id_mode")

    return LaunchDescription([

        DeclareLaunchArgument(
            "toml_path",
            default_value="",
            description="Path to model.toml (used by onnx_detector node).",
        ),

        DeclareLaunchArgument(
            "image_topic",
            default_value="/camera/camera/color/image_raw",
            description="Input image topic.",
        ),

        DeclareLaunchArgument(
            "detections_topic",
            default_value="/detections",
            description="Output detections base topic.",
        ),

        DeclareLaunchArgument(
            "publish_debug_image",
            default_value="true",
            description="Publish annotated image on <detections_topic>/image",
        ),

        DeclareLaunchArgument(
            "detections_qos",
            default_value="sensor_data",
            description="QoS for detections topic",
        ),

        DeclareLaunchArgument(
            "debug_qos",
            default_value="best_effort",
            description="QoS for debug image topic",
        ),

        DeclareLaunchArgument(
            "img_h",
            default_value="480",
            description="ONNX input height",
        ),

        DeclareLaunchArgument(
            "img_w",
            default_value="640",
            description="ONNX input width",
        ),

        DeclareLaunchArgument(
            "class_id_mode",
            default_value="name",
            description="Publish class_id as id or name",
        ),

        # ------------------------
        # 1) ONNX Detector
        # ------------------------
        Node(
            package="cartifactory",
            executable="onnx_detector_node",
            name="onnx_detector",
            output="screen",
            parameters=[{
                "toml_path": toml_path,
                "image_topic": image_topic,
                "detections_topic": detections_topic,
                "publish_debug_image": publish_debug_image,
                "detections_qos": detections_qos,
                "debug_qos": debug_qos,
                "img_h": img_h,
                "img_w": img_w,
                "class_id_mode": class_id_mode,
            }],
        ),

        # ------------------------
        # 2) Keyword Matcher
        # ------------------------
        Node(
            package="cartifactory",
            executable="keyword_matcher_node",
            name="keyword_matcher",
            output="screen",
            parameters=[{
                "detections_topic": detections_topic,
            }],
            emulate_tty=True,
        ),

        # ------------------------
        # 3) Pipeline Monitor
        # ------------------------
        Node(
            package="cartifactory",
            executable="pipeline_monitor",
            name="pipeline_monitor",
            output="screen",
            parameters=[{
                "detector_stats_topic": "/stats/detector",
                "matcher_stats_topic": "/stats/matcher",
                "pipeline_stats_topic": "/stats/pipeline",
                "image_topic": image_topic,
                "detections_topic": detections_topic,
                "fps_ema_alpha": 0.2,
                "latency_window_size": 30,
            }],
        ),

    ])