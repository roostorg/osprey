from osprey.worker.lib.singleton import Singleton
from osprey.worker.lib.singletons import CONFIG

from .lib.druid_client_holder import DruidClientHolder

DRUID: Singleton[DruidClientHolder] = Singleton(DruidClientHolder)

config = CONFIG.instance()
