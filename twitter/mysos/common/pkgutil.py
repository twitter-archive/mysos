import os

from twitter.common.dirutil import safe_mkdir

import pkg_resources


def unpack_assets(output_dir, module, asset_path, execute=lambda x: None):
  """
    Extract files from a module in a package into 'output_dir'.

    :param output_dir: The directory to copy the assets *root* to.
    :param module: The module in the package to find the assets in. e.g., twitter.mysos.executor.
    :param asset_path: The path of the asset relative to the module root to unpack.
    :param execute: A function that expects a single argument as the path to an asset file. It can
                    be used to process this file.

    NOTE: If the specified 'asset_path' is a directory, its contents are copied but it itself is not
          recreated in the output directory. e.g., <module_dir>/files/bin/file.sh is copied to
          <output_dir>/bin/file.sh. An analogy is that its like `cp -R asset_path/* output_dir` and
          not like `cp -R asset_path output_dir`.
  """
  _unpack_assets(output_dir, module, asset_path, execute, asset_path)


def _unpack_assets(output_dir, module, asset_root, execute, current_path):
  """
    The internal helper function for unpack_assets(...) recursion.
    :param current_path: Records the current
  """
  for asset in pkg_resources.resource_listdir(module, current_path):
    asset_target = os.path.join(os.path.relpath(current_path, asset_root), asset)
    if pkg_resources.resource_isdir(module, os.path.join(current_path, asset)):
      safe_mkdir(os.path.join(output_dir, asset_target))
      _unpack_assets(output_dir, module, asset_root, execute, os.path.join(current_path, asset))
    else:
      output_file = os.path.join(output_dir, asset_target)
      with open(output_file, 'wb') as fp:
        fp.write(pkg_resources.resource_string(
            module, os.path.join(asset_root, asset_target)))
        execute(output_file)
