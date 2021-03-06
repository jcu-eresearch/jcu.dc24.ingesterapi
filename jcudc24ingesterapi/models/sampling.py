from abc import abstractmethod
from jcudc24ingesterapi import typed, APIDomainObject

__author__ = 'Casey Bajema'


class _Sampling(APIDomainObject):
    """
    Base sampling object that defines the type.

    The intended usage is for the ingester platform to poll sample_now at regular intervals and run the
    ingester when it returns True.
    """

    @abstractmethod
    def sample_now(self, data_entry, dataset, datetime, poll_rate=1):
        """
        Test if the ingester should run at the given point in time specified by datetime.

        :param datetime: Should the ingester be run at the given time
        :param poll_rate: Time in milliseconds between calls to sample_now
        :return: True/False if the ingester should use the data at the time provided.
        """
        pass



class CustomSampling(_Sampling):
    """
    Use a custom python script to provide sampling times

    The maximum size of the script is defined by MAX_SCRIPT_SIZE
    """

    MAX_SCRIPT_SIZE = 1000000

    def __init__(self, script_handle):
        file = open(script_handle, 'r')
        # TODO: Throw an exception if the script is too large.
        self.script = script_handle

    def sample_now(self, datetime):
        self.datetime = datetime

        file = open(self.script, 'r')

        eval(file.read(self.MAX_SCRIPT_SIZE), poll_rate=1)

        return self.result


class RepeatSampling(_Sampling):
    """
    Sample based on a map of datetime/repeat rate pairs allowing the user to set an unlimited number of
    repeating times to sample on.
    """
    __xmlrpc_class__ = "repeat_sampling"
    # Repeat rates
    microsecond, millisecond, second, minute, hour, day, week, month, year = range(9)

    def __init__(self, sample_times):
        """
        :param sample_times: Map of datetime/repeat rate pairs.
        """
        # TODO: Validate the sample_times
        self.sample_times = sample_times

    def sample_now(self, datetime):
        # TODO: Implement the sampling times.
        pass

class PeriodicSampling(_Sampling):
    """
    Sample at a set rate in milliseconds
    """
    __xmlrpc_class__ = "periodic_sampling"
    rate = typed("_rate", int, "Rate of sampling in ms")

    def __init__(self, rate=None):
        self.rate = rate


class CronSampling(_Sampling):
    """
    Sample at a set rate in milliseconds
    """
    __xmlrpc_class__ = "cron_sampling"
    cron_string = typed("_cron_string", int, "Cron configuration string")

    def __init__(self, cron_string=None):
        self.cron_string = cron_string

