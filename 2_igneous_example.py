#############################################################################
# Author:
# Hao Zhai @ MiRA, CASIA
# Description:
# Re-chunk, Downsample, Mesh, Skeletonize a dataset
# by igneous-pipeline in a single local machine
# Date:
# Mar 30, 2022
# Reference:
# https://github.com/seung-lab/igneous
# https://github.com/seung-lab/zmesh
# https://github.com/seung-lab/kimimaro
# Variable:
# many parameters from each step, see comments and references...
#############################################################################


from taskqueue import LocalTaskQueue
import igneous.task_creation as tc
from cloudvolume import CloudVolume


# after this code, cloud_dir_raw could be delete
cloud_dir_raw = '/absolute/local/path/to/cloudvolume/layer'
# cloud_dir_norm = '/absolute/local/path/to/cloudvolume/layer'
cloud_dir = '/absolute/local/path/to/new/cloudvolume/layer'

tq = LocalTaskQueue(parallel=16)  # number of processes

# (note that ONLY 'image' layer can be normalized)
# cloud_dir_norm = '/home/zhaih/share/scn40_seg_test'

# # normalize (1st pass): generate luminace levels for each layer
# tasks = tc.create_luminance_levels_tasks(
#     'precomputed://file://' + cloud_dir_raw,
#     mip=0,
#     coverage_factor=0.01,
#     offset=(0,0,0)
# )
# tq.insert(tasks)
# tq.execute()
# # normalize (2nd pass): stretch value distribution to full coverage
# tasks = tc.create_contrast_normalization_tasks(
#     'precomputed://file://' + cloud_dir_raw,
#     'precomputed://file://' + cloud_dir_norm,
#     mip=0,
#     clip_fraction=0.01,
#     fill_missing=False,
#     translate=(0,0,0)
# )
# tq.insert(tasks)
# tq.execute()
# print("Normalization Done!")

# transfer: re-chunk and translate / offset
tasks = tc.create_transfer_tasks(
    src_layer_path='precomputed://file://' + cloud_dir_raw,
    dest_layer_path='precomputed://file://' + cloud_dir,
    chunk_size=(512, 512, 64),
    preserve_chunk_size=False,
    mip=0,
    bounds=None,
    fill_missing=True,
    translate=None,
    dest_voxel_offset=None,
    encoding=None, 
    compress='gzip',
    factor=None,
    skip_downsamples=True,
    sparse=False
)
tq.insert(tasks)
tq.execute()
print("Re-chunking Done!")

vol = CloudVolume('precomputed://file://' + cloud_dir)
vol.provenance.description = "Description of Data"
vol.provenance.owners = ['contact@email']
vol.commit_provenance()  # generates provenance json file

# downsample (1st pass): make anisotropy to isotropy
tasks = tc.create_downsampling_tasks(
    'precomputed://file://' + cloud_dir,
    mip=0,  # start downsampling from this mip level (writes to next level up)
    axis='z',
    num_mips=2,  # downsampling times
    compress='gzip',  # None or 'gzip' supports Neuroglancer 
    factor=(2, 2, 1),  # common options are (2,2,1) and (2,2,2)
)
tq.insert(tasks)
tq.execute()
# downsample (2nd pass): make small-view to large-view
tasks = tc.create_downsampling_tasks(
    'precomputed://file://' + cloud_dir,
    mip=2,
    axis='z',
    num_mips=3,
    compress='gzip',
    factor=(2, 2, 2),
)
tq.insert(tasks)
tq.execute()
print("Downsampling Done!")

# (note that ONLY 'segmentation' layer can be meshed and skeletonized)
# computing target for meshing and skeletonizaiton
target_mip = 2  # recommending isotropy mip
target_shape = (512, 512, 512)  # special chunk size for a process volume

# mesh (1st pass): process each chunk volume
tasks = tc.create_meshing_tasks(
    'precomputed://file://' + cloud_dir,
    mip=target_mip,
    shape=target_shape,
    mesh_dir='mesh_mip_{:d}'.format(target_mip)
)
tq.insert(tasks)
tq.execute()
# mesh (2nd pass): register all chunk volume
tasks = tc.create_mesh_manifest_tasks(
    'precomputed://file://' + cloud_dir,
    magnitude=3
)
tq.insert(tasks)
tq.execute()
print("Meshing Done!")

# skeletonize (1st pass): process each chunk volume
tasks = tc.create_skeletonizing_tasks(
    'precomputed://file://' + cloud_dir,
    mip=target_mip,
    shape=target_shape,
    teasar_params={
        'scale': 4, 
        'const': 500,
        'pdrf_exponent': 4,
        'pdrf_scale': 100000,
        'soma_detection_threshold': 1100,
        'soma_acceptance_threshold': 3500,
        'soma_invalidation_scale': 1.0,
        'soma_invalidation_const': 300,
        'max_paths': None
    },  # parameters from kimimaro (TEASAR)
    dust_threshold=1000
)
tq.insert(tasks)
tq.execute()
# skeletonize (2nd pass): merge all chunk volume
tasks = tc.create_unsharded_skeleton_merge_tasks(
    'precomputed://file://' + cloud_dir,
    magnitude=3,
    dust_threshold=1000,
    tick_threshold=3500
)
tq.insert(tasks)
tq.execute()
print("Skeletonization Done!")
