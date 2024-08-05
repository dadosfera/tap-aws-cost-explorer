"""Stream type classes for tap-aws-cost-explorer."""

import datetime
from pathlib import Path
from typing import Optional, Iterable

import pendulum
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_aws_cost_explorer.client import AWSCostExplorerStream

class CostAndUsageWithResourcesStream(AWSCostExplorerStream):
    """Define custom stream."""
    name = "cost"
    primary_keys = ["metric_name", "time_period_start"]
    replication_key = "time_period_start"

    schema = th.PropertiesList(
        th.Property("time_period_start", th.DateTimeType),
        th.Property("time_period_end", th.DateTimeType),
        th.Property("metric_name", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amount_unit", th.StringType),
    ).to_dict()

    def _get_end_date(self):
        if self.config.get("end_date") is None:
            return datetime.datetime.today() - datetime.timedelta(days=1)
        return th.cast(datetime.datetime, pendulum.parse(self.config["end_date"]))

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        next_page = True
        start_date = self.get_starting_timestamp(context)
        end_date = self._get_end_date()
        data = []
        count = 0

        response = self.conn.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime("%Y-%m-%d"),
                    'End': end_date.strftime("%Y-%m-%d")
                },
                Granularity=self.config.get("granularity"),
                Metrics=self.config.get("metrics"),
            )

        next_page = response.get("NextPageToken")
        data.extend(response['ResultsByTime'])
        count+=count
        print(f'count -> {count}')
        while next_page:
            response = self.conn.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime("%Y-%m-%d"),
                    'End': end_date.strftime("%Y-%m-%d")
                },
                Granularity=self.config.get("granularity"),
                Metrics=self.config.get("metrics"),
                NextPageToken=next_page
            )
            next_page = response.get("NextPageToken")
            data.extend(response['ResultsByTime'])
            count+=count
            print(f'count -> {count}')

        print(f"data -> {data}")

        for row in data:
            for k, v in row.get("Total").items():
                yield {
                    "time_period_start": row.get("TimePeriod").get("Start"),
                    "time_period_end": row.get("TimePeriod").get("End"),
                    "metric_name": k,
                    "amount": v.get("Amount"),
                    "amount_unit": v.get("Unit")
                }


class CostsByServicesStream(AWSCostExplorerStream):
    """Define custom stream."""
    name = "costs_by_services"
    primary_keys = ["metric_name", "time_period_start"]
    replication_key = "time_period_start"

    schema = th.PropertiesList(
        th.Property("time_period_start", th.DateTimeType),
        th.Property("time_period_end", th.DateTimeType),
        th.Property("metric_name", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amount_unit", th.StringType),
        th.Property("service", th.StringType),
    ).to_dict()

    def _get_end_date(self):
        if self.config.get("end_date") is None:
            return datetime.datetime.today() - datetime.timedelta(days=1)
        return th.cast(datetime.datetime, pendulum.parse(self.config["end_date"]))

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        next_page = True
        start_date = self.get_starting_timestamp(context)
        end_date = self._get_end_date()

        while next_page:
            response = self.conn.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime("%Y-%m-%d"),
                    'End': end_date.strftime("%Y-%m-%d")
                },
                Granularity=self.config.get("granularity"),
                Metrics= self.config.get("metrics"),
                Filter= {
                    'Dimensions':{
                        'Key': 'RECORD_TYPE',
                        'Values': self.config.get("record_types"),
                    }
                },
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    }
                ]
            )

            next_page = response.get("NextPageToken")

            for row in response.get("ResultsByTime"):
                for k in row.get("Groups"):
                    for i, j in k.get("Metrics").items():
                        yield {
                            "time_period_start": row.get("TimePeriod").get("Start"),
                            "time_period_end": row.get("TimePeriod").get("End"),
                            "metric_name": i,
                            "amount": j.get("Amount"),
                            "amount_unit": j.get("Unit"),
                            "service": k.get('Keys')[0],
                        }


class CostsByUsageTypeStream(AWSCostExplorerStream):
    """Define custom stream."""
    name = "costs_by_usage_type"
    primary_keys = ["metric_name", "time_period_start"]
    replication_key = "time_period_start"

    schema = th.PropertiesList(
        th.Property("time_period_start", th.DateTimeType),
        th.Property("time_period_end", th.DateTimeType),
        th.Property("metric_name", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amount_unit", th.StringType),
        th.Property("usage_type", th.StringType),
    ).to_dict()

    def _get_end_date(self):
        if self.config.get("end_date") is None:
            return datetime.datetime.today() - datetime.timedelta(days=1)
        return th.cast(datetime.datetime, pendulum.parse(self.config["end_date"]))

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        next_page = True
        start_date = self.get_starting_timestamp(context)
        end_date = self._get_end_date()

        while next_page:
            response = self.conn.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime("%Y-%m-%d"),
                    'End': end_date.strftime("%Y-%m-%d")
                },
                Granularity=self.config.get("granularity"),
                Metrics= self.config.get("metrics"),
                Filter= {
                    'Dimensions':{
                        'Key': 'RECORD_TYPE',
                        'Values': self.config.get("record_types"),
                    }
                },
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'USAGE_TYPE'
                    }
                ]
            )

            next_page = response.get("NextPageToken")

            for row in response.get("ResultsByTime"):
                for k in row.get("Groups"):
                    for i, j in k.get("Metrics").items():
                        yield {
                            "time_period_start": row.get("TimePeriod").get("Start"),
                            "time_period_end": row.get("TimePeriod").get("End"),
                            "metric_name": i,
                            "amount": j.get("Amount"),
                            "amount_unit": j.get("Unit"),
                            "usage_type": k.get('Keys')[0],
                        }