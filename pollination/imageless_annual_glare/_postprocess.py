from dataclasses import dataclass
from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.honeybee_radiance.post_process import AnnualGlareAutonomy
from pollination.path.copy import Copy

# input/output alias
from pollination.alias.inputs.schedule import schedule_csv_input


@dataclass
class ImagelessAnnualGlarePostprocess(GroupedDAG):
    """Prepare folder for imageless annual glare."""

    # inputs
    input_folder = Inputs.folder(
        description='Folder with DGP results before redistributing the '
        'results to the original grids.'
    )

    schedule = Inputs.file(
        description='Path to an annual schedule file. Values should be 0-1 separated '
        'by new line. If not provided an 8-5 annual schedule will be created.',
        path='schedule.txt', optional=True, alias=schedule_csv_input
    )

    glare_threshold = Inputs.float(
        description='A fractional number for the threshold of DGP above which '
        'conditions are considered to induce glare. This value is used when '
        'calculating glare autonomy (the fraction of hours in which the view is free '
        'of glare). Common values are 0.35 (Perceptible Glare), 0.4 (Disturbing '
        'Glare), and 0.45 (Intolerable Glare).', default=0.4,
        spec={'type': 'number', 'minimum': 0, 'maximum': 1}
    )

    results_folder = Inputs.folder(
        description='Results folder which will be copied to the postprocess '
        'subfolder to get the sun up hours and grid information.'
    )

    @task(template=Copy)
    def copy_results_folder(self, src=results_folder):
        return [
            {
                'from': Copy()._outputs.dst,
                'to': 'results'
            }
        ]

    @task(template=MergeFolderData)
    def restructure_daylight_glare_probability_results(
        self, input_folder=input_folder, extension='dgp'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results'
            }
        ]

    @task(
        template=AnnualGlareAutonomy,
        needs=[restructure_daylight_glare_probability_results]
    )
    def daylight_glare_autonomy(
        self,
        folder=restructure_daylight_glare_probability_results._outputs.output_folder,
        schedule=schedule, glare_threshold=glare_threshold
    ):
        return [
            {
                'from': AnnualGlareAutonomy()._outputs.annual_metrics,
                'to': 'metrics'
            }
        ]

    results = Outputs.folder(
        source='results', description='results folder.'
    )

    metrics = Outputs.folder(
        source='metrics', description='metrics folder.'
    )
