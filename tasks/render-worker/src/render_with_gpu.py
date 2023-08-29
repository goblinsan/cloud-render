import sys
import bpy
argv = sys.argv
if "--" in argv:
    argv = argv[argv.index("--") + 1:]  # get all args after "--"

    if len(argv) > 0:
        print(f"Using GPU {argv[0]} to render")
        gpu_name = argv[0]
        bpy.context.preferences.addons["cycles"].preferences.devices[gpu_name].use = True

else:
    print("No GPU found on the system to render.  Using blender's default handling")
