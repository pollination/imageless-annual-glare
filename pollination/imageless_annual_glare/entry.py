from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.wea import wea_input_timestep_check
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.radiancepar import rad_par_annual_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count
from pollination.alias.inputs.schedule import schedule_csv_input
from pollination.alias.outputs.daylight import glare_autonomy_results


from ._prepare_folder import ImagelessAnnualGlarePrepareFolder
from ._raytracing import ImagelessAnnualGlare
from ._postprocess import ImagelessAnnualGlarePostprocess


@dataclass
class ImagelessAnnualGlareEntryPoint(DAG):
    """Imageless annual glare entry point."""

    # inputs
    north = Inputs.float(
        default=0,
        description='A number for rotation from north.',
        spec={'type': 'number', 'minimum': 0, 'maximum': 360},
        alias=north_input
    )

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids. The default value is set to 1, which means that the '
        'cpu_count is always respected.', default=500,
        spec={'type': 'integer', 'minimum': 1},
        alias=min_sensor_count_input
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05',
        alias=rad_par_annual_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifier or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip'],
        alias=hbjson_model_grid_input
    )

    wea = Inputs.file(
        description='Wea file.',
        extensions=['wea'],
        alias=wea_input_timestep_check
    )

    schedule = Inputs.file(
        description='Path to an annual schedule file. Values should be 0-1 separated '
        'by new line. If not provided an 8-5 annual schedule will be created.',
        extensions=['txt', 'csv'], optional=True, alias=schedule_csv_input
    )

    glare_threshold = Inputs.float(
        description='A fractional number for the threshold of DGP above which '
        'conditions are considered to induce glare. This value is used when '
        'calculating glare autonomy (the fraction of hours in which the view is free '
        'of glare). Common values are 0.35 (Perceptible Glare), 0.4 (Disturbing '
        'Glare), and 0.45 (Intolerable Glare).',
        default=0.4,
        spec={'type': 'number', 'minimum': 0, 'maximum': 1}
    )

    luminance_factor = Inputs.float(
        description='Luminance factor in cd/m2. If the sky patch brightness is above '
        'this factor it will act as a glare source. If larger than 100, it is used as '
        'constant threshold in cd/m2. If less than or equal to 100, this factor '
        'multiplied by the average luminance in each view will be used as threshold for '
        'detecting the glare sources (not recommended). The default value is 2000 '
        '(fixed threshold method).',
        default=2000,
        spec={'type': 'number'}
    )

    @task(template=ImagelessAnnualGlarePrepareFolder)
    def prepare_folder_imageless_annual_glare(
        self, north=north, cpu_count=cpu_count,
        min_sensor_count=min_sensor_count,  grid_filter=grid_filter,
        model=model, wea=wea
    ):
        return [
            {
                'from': ImagelessAnnualGlarePrepareFolder()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': ImagelessAnnualGlarePrepareFolder()._outputs.resources,
                'to': 'resources'
            },
            {
                'from': ImagelessAnnualGlarePrepareFolder()._outputs.initial_results,
                'to': 'initial_results'
            },
            {
                'from': ImagelessAnnualGlarePrepareFolder()._outputs.sensor_grids
            }
        ]

    @task(
        template=ImagelessAnnualGlare,
        needs=[prepare_folder_imageless_annual_glare],
        loop=prepare_folder_imageless_annual_glare._outputs.sensor_grids,
        # create a subfolder for each grid
        sub_folder='initial_results/{{item.full_id}}',
        # sensor_grid sub_path
        sub_paths={
            'octree_file': 'scene.oct',
            'sensor_grid': 'grid/{{item.full_id}}.pts',
            'sky_matrix': 'sky.mtx',
            'sky_dome': 'sky.dome',
            'bsdfs': 'bsdf'
            }
    )
    def annual_imageless_glare(
        self,
        radiance_parameters=radiance_parameters,
        octree_file=prepare_folder_imageless_annual_glare._outputs.resources,
        grid_name='{{item.full_id}}',
        sensor_grid=prepare_folder_imageless_annual_glare._outputs.resources,
        sensor_count='{{item.count}}',
        sky_matrix=prepare_folder_imageless_annual_glare._outputs.resources,
        sky_dome=prepare_folder_imageless_annual_glare._outputs.resources,
        bsdfs=prepare_folder_imageless_annual_glare._outputs.model_folder,
        luminance_factor=luminance_factor
    ):
        pass

    @task(
        template=ImagelessAnnualGlarePostprocess,
        needs=[prepare_folder_imageless_annual_glare, annual_imageless_glare],
        sub_paths={
            'input_folder': 'dgp',
            'grids_info': 'grids_info.json',
            'sun_up_hours': 'sun-up-hours.txt'
            }
    )
    def postprocess_imageless_annual_glare(
        self, input_folder=prepare_folder_imageless_annual_glare._outputs.initial_results,
        schedule=schedule, glare_threshold=glare_threshold,
        grids_info=prepare_folder_imageless_annual_glare._outputs.resources,
        sun_up_hours=prepare_folder_imageless_annual_glare._outputs.resources
    ):
        return [
            {
                'from': ImagelessAnnualGlarePostprocess()._outputs.results,
                'to': 'results'
            },
            {
                'from': ImagelessAnnualGlarePostprocess()._outputs.metrics,
                'to': 'metrics'
            }
        ]

    results = Outputs.folder(
        source='results', description='Folder with raw result files (.dgp) '
        'that contain matrices for the daylight glare probability.'
    )

    ga = Outputs.folder(
        source='metrics/ga', description='Glare autonomy results.',
        alias=glare_autonomy_results
    )
