def generate_data_folder_structure():
    """
    Create the required folder structure for the cayman_office_system data module.
    This includes the 'data-cos' root and all subdirectories as defined in path_director.py.
    """
    import os

    base_dir = 'data-cos'
    subdirs = [
        'dataset-sector',
        'dataset-market',
        'dataset-order',
        'dataset-balance',
        'dataset-trade',
        'dataset-status',
        'dataset-holding',
        'dataset-currency',
        'dataset-generate',
        'dataset-bbg',
        'dataset-account',
        'dataset-account/lkef-master',
        'dataset-account/lkef-feeder1',
        'dataset-account/lkef-feeder2',
        'dataset-account/dataset-account-raw',
        'dataset-operation',
    ]

    # Create the root data directory only if it does not exist
    created_any = False
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        print(f"| Created root data directory: '{base_dir}'")
        created_any = True

    # Create all required subdirectories if they do not exist
    for subdir in subdirs:
        subdir_path = os.path.join(base_dir, subdir)
        if not os.path.exists(subdir_path):
            os.makedirs(subdir_path)
            print(f"| Created subdirectory: '{subdir_path}'")
            created_any = True

    if not created_any:
        print(f"| Data folder structure already exists.")