#############################################################################
# Author:
# Hao Zhai @ MiRA, CASIA
# Description:
# Visualize a precomputed dataset in LAN by a local Neuroglancer
# Date:
# Mar 30, 2022
# Reference:
# https://github.com/google/neuroglancer/tree/master/python/examples
# Variable:
# host, port, cv_list, s.layers['...']
#############################################################################


import argparse
import time
from threading import Thread
from functools import partial
from concurrent.futures import ProcessPoolExecutor

import neuroglancer
import neuroglancer.cli
from cloudvolume import CloudVolume
from cloudvolume.server import view
from cloudvolume.lib import red


def _cloudvolume_process(cv, cv_link_list, port_list):
    vol = CloudVolume(cv_link_list[cv])
    view(vol.cloudpath, hostname='0.0.0.0', port=port_list[cv])
    # original CloudVolume.viewer locked hostname for security reason

class InteractiveInference(object):
    def __init__(self, port, host, cv_list):
        viewer = self.viewer = neuroglancer.Viewer()

        viewer.actions.add('reload-cloudvolume', self._reload_cloudvolume_action)
        with viewer.config_state.txn() as s:
            # design 'Keyboard R' as reloading all CloudVolume
            s.input_event_bindings.data_view['keyr'] = 'reload-cloudvolume'

        self.cv_list = cv_list
        self.cv_num = len(cv_list)
        self.port_list = list(range(port, port + self.cv_num))

        with viewer.txn() as s:
            p = 0
            # a example of image layer
            s.layers['image'] = neuroglancer.ImageLayer(
                source='precomputed://' + host + str(self.port_list[p]),
                opacity=float(1.0)
            ); p += 1
            # a example of segmentation layer
            s.layers['segmentation'] = neuroglancer.SegmentationLayer(
                source={
                    'url': 'precomputed://' + host + str(self.port_list[p]),
                    # a example of subsources selection
                    'subsources': {
                        'default': True,
                        'bounds': True,
                        'properties': True,
                        'mesh': True,
                        'skeletons': False
                    },
                    'enableDefaultSubsources': False
                }
            ); p += 1
            # another example of RGB image layer
            # s.layers['vesicle'] = neuroglancer.ImageLayer(
            #     source='precomputed://' + host + str(self.port_list[p]),
            #     opacity=float(0.3),
            #     blend='additive',
            #     shader='''
            #         #uicontrol invlerp normalized
            #         void main () {
            #         emitRGB(vec3(normalized(getDataValue()), 0, 0));
            #         }
            #     ''',
            # ); p += 1
            # s.layers['vesicle'].visible = False
        
        # print the url in red, using by Chrome or FireFox
        print(red(host + str(viewer).split(':')[-1]))

        self._reload_cloudvolume()

    def _reload_cloudvolume(self):
        try:
            self.pool.terminate()
        except:
            pass
        self.pool = ProcessPoolExecutor(max_workers=self.cv_num)
        p_partial = partial(_cloudvolume_process, \
            cv_link_list=cv_list, port_list=self.port_list)
        self.pool.map(p_partial, list(range(self.cv_num)))
        self.pool.close()
        self.pool.join()
    
    def _reload_cloudvolume_action(self, action_state):
        t = Thread(target=self._reload_cloudvolume)
        t.daemon = True; t.start()


host = 'http://' + '192.168.3.33' + ':'  # host address in LAN
port = 10001  # starts of ports
cv_list = [
    'precomputed://file://' + '/absolute/local/path/to/image/layer',
    'precomputed://file://' + '/absolute/local/path/to/segmentation/layer',
    # 'precomputed://file://' + '/absolute/local/path/to/vesicle/layer',
]  # list of CloudVolume, keep the same order as s.layers['...']

ap = argparse.ArgumentParser()
neuroglancer.cli.add_server_arguments(ap)
args = ap.parse_args()
args.bind_address = '0.0.0.0'  # need to be open in LAN
neuroglancer.cli.handle_server_arguments(args)

inf = InteractiveInference(port, host, cv_list)

while True:
    time.sleep(1000)  # hanging except Ctrl+C
