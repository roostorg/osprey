from enum import StrEnum, auto


class InputStreamSource(StrEnum):
    """Where data for the input to the classification engine are sourced."""

    PUBSUB = auto()
    """
    Sources events from pubsub
    """

    OSPREY_COORDINATOR = auto()
    """
    Sources events from the osprey coordinator
    """

    SYNTHETIC = auto()
    """Creates synthetic events in order to populate the output sinks with some sample data for local development."""

    KAFKA = auto()
    """Sources events from kafka."""

    PLUGIN = auto()
    """Sources events from whatever a plugin defines via register_input_stream."""


class OutputSinkDestination(StrEnum):
    """Where the data of a classified event should be sent."""

    OSPREY = auto()
    """Processes the output by sending the classification results to kafka for indexing in druid, scylla for execution
    result storage, and the labels service / webhook consumers for event effects."""

    STDOUT = auto()
    """Prints the output to standard out. Good for local development or debugging."""
