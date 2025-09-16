from ..propagation.baggage import HTTPBaggagePropagator
from .baggage import BaggageManager

baggage_manager = BaggageManager()
baggage_propagator = HTTPBaggagePropagator()
