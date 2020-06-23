from banzai.stages import Stage
from banzai.frames import ObservationFrame


class PoissonInitializer(Stage):
    def do_stage(self, image) -> ObservationFrame:
        image.primary_hdu.init_poisson_uncertainties()
        return image
