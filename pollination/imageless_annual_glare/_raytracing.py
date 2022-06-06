"""Raytracing DAG for imageless annual glare."""

from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.coefficient import DaylightCoefficientNoSkyMatrix
from pollination.honeybee_radiance.glare import DCGlareDGP


@dataclass
class ImagelessAnnualGlare(DAG):
    # inputs
    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing',
        default='-ab 2 -ad 5000 -lw 2e-05'
    )

    octree_file = Inputs.file(
        description='A Radiance octree file.',
        extensions=['oct']
    )

    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.ill'
    )

    sensor_grid = Inputs.file(
        description='Sensor grid file.',
        extensions=['pts']
    )

    sensor_count = Inputs.int(
        description='Number of sensors in the input sensor grid.'
    )

    sky_matrix = Inputs.file(
        description='Path to total sky matrix file.'
    )

    sky_dome = Inputs.file(
        description='Path to sky dome file.'
    )

    bsdfs = Inputs.folder(
        description='Folder containing any BSDF files needed for ray tracing.',
        optional=True
    )

    luminance_factor = Inputs.float(
        description='Luminance factor in cd/m2. If the sky patch brightness is above '
        'this factor it will act as a glare source.', default=2000
    )

    @task(template=DaylightCoefficientNoSkyMatrix)
    def direct_sky(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -ab 1 -c 1',
        output_format='f',
        sensor_count=sensor_count,
        sky_dome=sky_dome,
        sensor_grid=sensor_grid,
        scene_file=octree_file,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightCoefficientNoSkyMatrix()._outputs.result_file,
                'to': 'dc_direct.mtx'
            }
        ]

    @task(template=DaylightCoefficientNoSkyMatrix)
    def total_sky(
        self,
        radiance_parameters=radiance_parameters,
        fixed_radiance_parameters='-aa 0.0 -I -c 1',
        output_format='f',
        sensor_count=sensor_count,
        sky_dome=sky_dome,
        sensor_grid=sensor_grid,
        scene_file=octree_file,
        bsdf_folder=bsdfs
    ):
        return [
            {
                'from': DaylightCoefficientNoSkyMatrix()._outputs.result_file,
                'to': 'dc_total.mtx'
            }
        ]

    @task(
        template=DCGlareDGP,
        needs=[total_sky, direct_sky]
    )
    def daylight_glare_probability(
        self,
        name=grid_name,
        dc_direct=direct_sky._outputs.result_file,
        dc_total=total_sky._outputs.result_file,
        sky_vector=sky_matrix,
        view_rays=sensor_grid,
        threshold_factor=luminance_factor
    ):
        return [
            {
                'from': DCGlareDGP()._outputs.view_rays_dgp,
                'to': '../dgp/{{self.name}}.dgp'
            }
        ]
