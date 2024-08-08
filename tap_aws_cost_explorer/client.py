"""Custom client handling, including AWSCostExplorerStream base class."""

import boto3

from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap
import singer
from datetime import datetime, timedelta
import singer
from singer import utils

LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    "access_key",
    "secret_key",
    "start_date",
    "granularity",
    "metrics",
    "record_types"]


class AWSCostExplorerStream(Stream):
    """Stream class for AWSCostExplorer streams."""

    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.conn = boto3.client(
            'ce',
            aws_access_key_id=self.config.get("access_key"),
            aws_secret_access_key=self.config.get("secret_key"),
            aws_session_token=self.config.get("session_token"),
        )
        self.state = self.get_state()

    def get_bookmark(self):
        if (self.state is None) or ("bookmarks" not in self.state):
            return None
        bookmarks = self.state.get("bookmarks", {})
        stream_bookmark = bookmarks.get(self.tap_stream_id, {})
        LOGGER.info("last_value:")
        
        if "last_value" in stream_bookmark:
            LOGGER.info(stream_bookmark["last_value"])
            return stream_bookmark["last_value"]

    def _write_state_message(self):

     
        if "bookmarks" not in self.state:
            self.state["bookmarks"] = {}
        
        LOGGER.info(f"Write_bookmark Input Value: {value}")
        value =  datetime.now() - timedelta(days=7)
        LOGGER.info(f"After count write_bookmakrk -> {value}")
        value = value.strftime("%Y-%m-%d")
        LOGGER.info(f"After transformation -> {value}")

        value_dict = {"last_value": value}

        LOGGER.info(f"Write_bookmark Output Value: {value}")

        self.state["bookmarks"][self.tap_stream_id] = value_dict
        
        singer.write_state(self.state)

    @utils.handle_top_exception(LOGGER)
    def get_state(self):

        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        state = {}
        if args.state:
            state = args.state

        LOGGER.info(f"State: {state}")
        return state
