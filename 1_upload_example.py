#############################################################################
# Author:
# Hao Zhai @ MiRA, CASIA
# Description:
# Uploading a dataset for CloudVloume by a single machine 
# Date:
# Mar 30, 2022
# Reference:
# https://github.com/seung-lab/cloud-volume/wiki
# Variable:
# cloud_dir, info, provenance,
# image_dir, image_name, max_workers...
#############################################################################


from os import listdir, path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import imageio
# import h5py

from cloudvolume import CloudVolume
from cloudvolume.lib import mkdir, touch


cloud_dir = '/absolute/local/path/to/cloudvolume/layer'

info = CloudVolume.create_new_info(
	num_channels = 1,
	layer_type = 'image',  # 'image' or 'segmentation'
	data_type = 'uint8',
	encoding = 'raw',  # 'raw' for uncompressed, and recommending
    # 'jpeg' for image, 'compressed_segmentation' for segmentation (uint32 or 64)
	resolution = [ 5, 5, 40 ],  # X,Y,Z values in nanometers
	voxel_offset = [ 0, 0, 0 ],  # values X,Y,Z values in voxels
	chunk_size = [ 2048, 2048, 1 ],  # rechunk of image X,Y,Z in voxels
	volume_size = [ 10000, 10000, 1250 ],  # X,Y,Z size in voxels
)

vol = CloudVolume('file://' + cloud_dir, info=info)
vol.commit_info()  # generates info json file

image_dir = 'local/path/to/images'

progress_dir = mkdir(path.join(cloud_dir, 'progress/'))
done_files = set([ int(z) for z in listdir(progress_dir) ])
all_files = set(range(vol.bounds.minpt.z, vol.bounds.maxpt.z + 1))

to_upload = [ int(z) for z in list(all_files.difference(done_files)) ]
to_upload.sort()

def process(z):
    image_name = '{:04d}.tif'.format(z)
    image = imageio.imread(path.join(image_dir, image_name))
    # another example for h5 file
    # image = h5py.File(path.join(image_dir, image_name), 'r')['data'][:] 
    image = image.transpose()  # need to fit X,Y axis
    image = image[..., np.newaxis]  # need a newaxis for channels
    vol[:, :, z] = image  # contain encoding procedure
    touch(path.join(progress_dir, str(z)))

with ProcessPoolExecutor(max_workers=8) as executor:
    executor.map(process, to_upload)
