from pollination.imageless_annual_glare.entry import ImagelessAnnualGlareEntryPoint
from queenbee.recipe.dag import DAG


def test_imageless_annual_glare():
    recipe = ImagelessAnnualGlareEntryPoint().queenbee
    assert recipe.name == 'imageless-annual-glare-entry-point'
    assert isinstance(recipe, DAG)
