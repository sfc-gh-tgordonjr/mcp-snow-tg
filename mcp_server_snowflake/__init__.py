# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Snowflake MCP Server Package.

This package provides a Model Context Protocol (MCP) server implementation for
interacting with Snowflake's Cortex AI services. The server enables seamless
integration with Snowflake's machine learning and AI capabilities through a
standardized protocol interface.

The package supports:
- Cortex Complete: Large language model completions and chat
- Cortex Search: Semantic search across Snowflake data
- Cortex Analyst: Natural language to SQL query generation
- Model discovery: Identify available models in region

The server can be configured through command-line arguments or environment
variables and uses a YAML configuration file to define service specifications.

Environment Variables
---------------------
SNOWFLAKE_ACCOUNT : str
    Snowflake account identifier (alternative to --account-identifier)
SNOWFLAKE_USER : str
    Snowflake username (alternative to --username)
SNOWFLAKE_PAT : str
    Programmatic Access Token (alternative to --pat)
SERVICE_CONFIG_FILE : str
    Path to service configuration file (alternative to --service-config-file)

"""

import asyncio
import argparse
import os

from . import server
from mcp_server_snowflake.utils import MissingArgumentsException


def get_var(var_name: str, env_var_name: str, args) -> str | None:
    """
    Retrieve variable value from command line arguments or environment variables.

    Checks for a variable value first in command line arguments, then falls back
    to environment variables. This provides flexible configuration options for
    the MCP server.

    Parameters
    ----------
    var_name : str
        The attribute name to check in the command line arguments object
    env_var_name : str
        The environment variable name to check if command line arg is not provided
    args : argparse.Namespace
        Parsed command line arguments object

    Returns
    -------
    str | None
        The variable value if found in either source, None otherwise

    Examples
    --------
    Get account identifier from args or environment:

    >>> args = parser.parse_args(['--account-identifier', 'myaccount'])
    >>> get_var('account_identifier', 'SNOWFLAKE_ACCOUNT', args)
    'myaccount'

    >>> os.environ['SNOWFLAKE_ACCOUNT'] = 'myaccount'
    >>> args = parser.parse_args([])
    >>> get_var('account_identifier', 'SNOWFLAKE_ACCOUNT', args)
    'myaccount'
    """

    if getattr(args, var_name):
        return getattr(args, var_name)
    elif env_var_name in os.environ:
        return os.environ[env_var_name]
    else:
        return None


def main():
    """
    Main entry point for the Snowflake MCP server package.

    Parses command line arguments, retrieves configuration from arguments or
    environment variables, validates required parameters, and starts the
    asyncio-based MCP server. The server handles Model Context Protocol
    communications over stdin/stdout streams.

    The function sets up argument parsing for Snowflake connection parameters
    and service configuration, then delegates to the main server implementation.

    Raises
    ------
    MissingArgumentException
        If required parameters (account_identifier and pat) are not provided
        through either command line arguments or environment variables
    SystemExit
        If argument parsing fails or help is requested

    Notes
    -----
    The server requires these minimum parameters:
    - account_identifier: Snowflake account identifier
    - username: Snowflake username
    - pat: Programmatic Access Token for authentication
    - service-config-file: Path to service configuration file

    """
    parser = argparse.ArgumentParser(description="Snowflake MCP Server")

    parser.add_argument(
        "--account-identifier", required=False, help="Snowflake account identifier"
    )
    parser.add_argument(
        "--username", required=False, help="Username for Snowflake account"
    )
    parser.add_argument(
        "--pat", required=False, help="Programmatic Access Token (PAT) for Snowflake"
    )
    parser.add_argument(
        "--service-config-file",
        required=False,
        help="Path to service specification file",
    )

    args = parser.parse_args()
    account_identifier = get_var("account_identifier", "SNOWFLAKE_ACCOUNT", args)
    username = get_var("username", "SNOWFLAKE_USER", args)
    pat = get_var("pat", "SNOWFLAKE_PAT", args)
    service_config_file = get_var("service_config_file", "SERVICE_CONFIG_FILE", args)

    parameters = dict(
        account_identifier=account_identifier,
        username=username,
        pat=pat,
        service_config_file=service_config_file,
    )

    if not all(parameters.values()):
        raise MissingArgumentsException(
            missing=[k for k, v in parameters.items() if not v]
        ) from None
    asyncio.run(
        server.main(
            account_identifier=account_identifier,
            username=username,
            pat=pat,
            config_path=service_config_file,
        )
    )


# Optionally expose other important items at package level
__all__ = ["main", "server"]
