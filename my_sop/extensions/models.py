from django.db import models
from datetime import datetime
from time import strftime
from django.core import checks, exceptions, validators
from django.utils.translation import gettext_lazy as _
from django.utils.functional import Promise, cached_property
from django.db import connection, connections, router
from django import forms

#
# Custom field types in here.
#
class UnixTimestampField(models.DateTimeField):
    """UnixTimestampField: creates a DateTimeField that is represented on the
    database as a TIMESTAMP field rather than the usual DATETIME field.
    """

    def __init__(self, null=False, **kwargs):
        super(UnixTimestampField, self).__init__(**kwargs)
        # default for TIMESTAMP is NOT NULL unlike most fields, so we have to
        # cheat a little:
        self.isnull = null
        self.null = True  # To prevent the framework from shoving in "not null".

    def db_type(self, connection):
        typ = ['TIMESTAMP']
        # See above!
        if self.isnull:
            typ += ['NULL']
        if self.auto_created:
            typ += ['default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP']
        return ' '.join(typ)

    def to_python(self, value):
        return value

    def get_db_prep_value(self, value, connection, prepared=False):
        if value == None:
            return None
        return strftime('%Y%m%d%H%M%S', value.timetuple())

class BooleanField(models.BooleanField):
    def db_type(self, connection):
        typ = super().db_type(connection)
        if self.has_default():
            default = self.get_default()
            if type(typ) == str:
                typ += ' default ' + str(default)
            else:
                typ = 'default ' + str(default)
        return typ

class CharField(models.CharField):
    def db_type(self, connection):
        typ = super().db_type(connection)
        if self.has_default():
            default = self.get_default()
            typ += ' default "' + str(default) + '"'
        return typ

class IntegerField(models.IntegerField):
    def db_type(self, connection):
        typ = super().db_type(connection)
        if self.has_default():
            default = self.get_default()
            if type(typ) == str:
                typ += ' default ' + str(default)
            else:
                typ = 'default ' + str(default)
        return typ

class FloatField(models.FloatField):
    def db_type(self, connection):
        typ = super().db_type(connection)
        if self.has_default():
            default = self.get_default()
            if type(typ) == str:
                typ += ' default ' + str(default)
            else:
                typ = 'default ' + str(default)
        return typ

class ForeignKey(models.ForeignKey):
    def to_python(self, value):
        return value