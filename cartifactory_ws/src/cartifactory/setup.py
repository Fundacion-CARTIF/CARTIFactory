from setuptools import find_packages, setup
import os
from glob import glob
package_name = 'cartifactory'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.launch.py')),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Guillermo Cabezas',
    maintainer_email='guicab@cartif.es',
    description='TODO: Package description',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'onnx_detector_node = cartifactory.onnx_detector:main', 
            'keyword_matcher_node = cartifactory.keyword_matcher_node:main',
            'pipeline_monitor = cartifactory.pipeline_monitor:main',  
        ],
    },
)
