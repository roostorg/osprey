from enum import Enum


class OspreyAnalyticsEvents(str, Enum):
    EXTRACTED_FEATURES = 'osprey_extracted_features'
    ACTION_CLASSIFICATION = 'osprey_action_classification'
    EXPERIMENT_EXPOSURE_EVENT = 'osprey_experiment_exposure_event'
    LABEL_MUTATIONS = 'osprey_label_mutations'
    BULK_LABEL_JOB = 'osprey_bulk_label_job'
    RULES_VISUALIZER_GEN_GRAPH = 'network_action_osprey_rules_visualizer_generate_graph'



# There are more types, currently listing the ones we need to use in code
class EntityType(str, Enum):
    USER = 'User'
    GUILD = 'Guild'
