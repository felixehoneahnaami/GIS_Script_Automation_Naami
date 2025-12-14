import sys
import os
import arcpy
from arcpy.sa import *  # Spatial Analyst

# ===============================
# Hardcoded values for testing (Delete after testing)
# ===============================
input_folder = r"I:\_2025\EmmaPepprah\Subika_Faults_UTM\Subika_Faults_UTM"  # Folder containing multiple DXF files
input_projection = 32630  # EPSG code for WGS 1984 UTM Zone 30N
output_projection = 25000  # EPSG code for Ghana Metre Grid
output_folder = r"I:\_2025\EmmaPepprah\Subika_Faults_UTM\output"

# Optional parameters for testing
cell_size = 10  # Example cell size
cell_assignment_method = "MAXIMUM_HEIGHT"
resampling_type = "BILINEAR"
processing_extent = ""  # Leave empty if not required
# ===============================

# ===============================
# Parameters for manual input in ArcGIS Pro (Uncomment these after testing)
# ===============================
# input_folder = arcpy.GetParameterAsText(0)  # Folder containing multiple DXF files
# input_projection = arcpy.GetParameterAsText(1)  # Input projection (for defining DXF projection)
# output_projection = arcpy.GetParameterAsText(2)  # Output projection (for final TIFF file)
# output_folder = arcpy.GetParameterAsText(3)  # Output folder for saving TIFF

# Optional parameters
# cell_size = arcpy.GetParameterAsText(4)  # Cell size for final raster
# cell_assignment_method = arcpy.GetParameterAsText(5)  # Cell assignment method
# resampling_type = arcpy.GetParameterAsText(6)  # Resampling method for projection
# processing_extent = arcpy.GetParameterAsText(7)  # Processing extent (optional)
# ===============================

# Use default workspace and scratch geodatabase
base_folder = arcpy.env.workspace
scratch_gdb = arcpy.env.scratchGDB

# Ensure output folder exists
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Define spatial references
input_spatial_ref = arcpy.SpatialReference(input_projection)
output_spatial_ref = arcpy.SpatialReference(output_projection)

def process_file(input_dxf, cell_size):
    dynamic_name = os.path.basename(input_dxf).replace(".dxf", "")
    print(f"Processing file: {dynamic_name}")

    if not arcpy.Exists(input_dxf):
        print(f"Input DXF file not found at: {input_dxf}")
        return

    if not arcpy.Describe(input_dxf).spatialReference:
        arcpy.management.DefineProjection(input_dxf, input_spatial_ref)
        print(f"Defined projection for DXF: {input_projection}")

    arcpy.env.workspace = input_dxf
    multipatch_layers = arcpy.ListFeatureClasses(feature_type='MultiPatch')

    if not multipatch_layers:
        print(f"No multipatch layer found in the DXF file: {dynamic_name}")
        return
    multipatch_layer = multipatch_layers[0]
    print(f"Found multipatch layer: {multipatch_layer}")

    if not cell_size:
        desc = arcpy.Describe(multipatch_layer)
        cell_size = desc.extent.width / 1000 or 10

    multipatch_raster = os.path.join(scratch_gdb, f"{dynamic_name}_Multipa_Deep1")
    adjusted_raster = os.path.join(scratch_gdb, f"{dynamic_name}_Adjusted")
    projected_raster = os.path.join(scratch_gdb, f"{dynamic_name}_Projected")
    final_output = os.path.join(output_folder, f"{dynamic_name}_FinalOutput.tif")

    if arcpy.Exists(multipatch_raster):
        arcpy.Delete_management(multipatch_raster)
        print(f"Deleted existing multipatch raster: {multipatch_raster}")

    arcpy.conversion.MultipatchToRaster(
        multipatch_layer,
        multipatch_raster,
        cell_size=cell_size,
        cell_assignment_method=cell_assignment_method or "MAXIMUM_HEIGHT"
    )
    print(f"Multipatch to Raster complete: {multipatch_raster}")

    if output_spatial_ref.name == "AUGNG":
        adjusted_raster_result = Raster(multipatch_raster) + 1000
        print(f"Added 1000 to raster for AUGNG projection.")
    elif input_spatial_ref.name == "AUGNG" and output_spatial_ref.name != "AUGNG":
        adjusted_raster_result = Raster(multipatch_raster) - 1000
        print(f"Subtracted 1000 from raster when converting from AUGNG to another projection.")
    else:
        adjusted_raster_result = Raster(multipatch_raster)
        print(f"No adjustment applied as the projection is neither AUGNG nor converting to/from AUGNG.")

    adjusted_raster_result.save(adjusted_raster)
    print(f"Raster Calculator adjustment complete: {adjusted_raster}")

    raster_desc = arcpy.Describe(adjusted_raster)
    if not raster_desc.spatialReference or raster_desc.spatialReference.name == "Unknown":
        arcpy.management.DefineProjection(adjusted_raster, input_spatial_ref)
        print(f"Defined projection for raster: {input_projection}")

    original_extent = arcpy.env.extent
    if processing_extent:
        arcpy.env.extent = processing_extent
        print(f"Processing extent set to: {processing_extent}")

    if arcpy.Exists(final_output):
        arcpy.Delete_management(final_output)
        print(f"Deleted existing output file: {final_output}")

    arcpy.management.ProjectRaster(
        adjusted_raster,
        projected_raster,
        output_spatial_ref,
        resampling_type=resampling_type or "BILINEAR"
    )
    print(f"Raster reprojected to {output_projection}: {projected_raster}")

    arcpy.management.CopyRaster(
        projected_raster,
        final_output,
        format="TIFF"
    )
    print(f"Final output saved: {final_output}")

    for temp_file in [multipatch_raster, adjusted_raster, projected_raster]:
        if arcpy.Exists(temp_file):
            arcpy.Delete_management(temp_file)

    arcpy.env.extent = original_extent

def main():
    arcpy.env.overwriteOutput = True

    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
    else:
        print("Spatial Analyst extension is unavailable.")
        raise RuntimeError("Spatial Analyst extension is unavailable.")

    dxf_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.dxf')]
    if not dxf_files:
        print("No DXF files found in the input folder.")
        return

    print(f"Found {len(dxf_files)} DXF files. Starting batch processing...")
    for dxf_file in dxf_files:
        process_file(dxf_file, cell_size)

    arcpy.CheckInExtension("Spatial")

if __name__ == '__main__':
    main()
