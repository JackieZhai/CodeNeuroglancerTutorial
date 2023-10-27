

















tasks = tc.create_skeletonizing_tasks(
    'precomputed://file://' + cloud_dir,
    mip=target_mip,
    shape=target_shape,
    sharded=True,
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

tq = LocalTaskQueue(parallel=1)
tasks = tc.create_sharded_skeleton_merge_tasks(
    'precomputed://file://' + cloud_dir,
    dust_threshold=1000,
    tick_threshold=3500
)
tq.insert(tasks)
tq.execute()
print("Skeletonization Done!")