# imageless-annual-glare

Run an annual glare study for a Honeybee model to compute hourly Daylight Glare
Probability (DGP) for each sensor in a model's sensor grids.

This recipe uses the image-less glare method developed by Nathaniel Jones to
estimate glare at each sensor. [More information on this method can be found here](https://github.com/nljones/Accelerad/wiki/The-Imageless-Method-for-Spatial-and-Annual-Glare-Analysis).

The resulting DGP is used to compute Glare Autonomy (GA), which is the percentage
of occupied time that a view is free of glare.
