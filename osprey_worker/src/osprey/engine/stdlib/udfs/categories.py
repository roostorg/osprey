from enum import Enum


# this needs to be combed thru
class UdfCategories(str, Enum):
    DATETIME = 'Datetime'
    DNS = 'DNS'
    EMAIL = 'Email'
    ENCODING = 'Encoding'
    ENGINE = 'Engine'
    ENTITY = 'Entity'
    HASH = 'Hash'
    HTTP = 'HTTP'
    IP = 'IP'
    PHONE = 'Phone'
    RANDOM = 'Random'
    STRING = 'String'
