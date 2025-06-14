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
import logging
from typing import Optional, Dict, Any
from pydantic import AnyUrl
import yaml
import json
from pathlib import Path
from datetime import datetime
from functools import wraps

from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.types as types
import mcp.server.stdio
from snowflake.connector import connect

import mcp_server_snowflake.tools as tools
from mcp_server_snowflake.utils import SnowflakeObjectManager

config_file_uri = Path(__file__).parent.parent / "services" / "service_config.yaml"
server_name = "mcp-server-snowflake"
server_version = "0.0.1"
tag_major_version = 1
tag_minor_version = 0

logger = logging.getLogger(server_name)

class QueryExecutionError(Exception):
    """Custom exception for query execution errors"""
    pass

def log_query_execution(func):
    """Decorator to log query execution details"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        query = kwargs.get('query', '')
        start_time = datetime.now()
        
        try:
            result = await func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(
                f"Query executed successfully in {duration:.2f}s\n"
                f"Query: {query[:100]}{'...' if len(query) > 100 else ''}"
            )
            return result
            
        except Exception as e:
            logger.error(
                f"Query execution failed\n"
                f"Query: {query[:100]}{'...' if len(query) > 100 else ''}\n"
                f"Error: {str(e)}"
            )
            raise QueryExecutionError(f"Query execution failed: {str(e)}")
    
    return wrapper

class DDLExecutionError(Exception):
    """Custom exception for DDL execution errors"""
    pass

def log_ddl_execution(func):
    """Decorator to log DDL execution details"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        ddl_statement = kwargs.get('ddl_statement', '')
        object_path = kwargs.get('object_path', 'Not specified')
        start_time = datetime.now()
        
        try:
            result = await func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Extract operation type for better logging
            operation = "DDL"
            if ddl_statement.upper().startswith('CREATE'):
                operation = "CREATE"
            elif ddl_statement.upper().startswith('ALTER'):
                operation = "ALTER"
            elif ddl_statement.upper().startswith('DROP'):
                operation = "DROP"
            elif ddl_statement.upper().startswith('UPDATE'):
                operation = "UPDATE"
            elif ddl_statement.upper().startswith('INSERT'):
                operation = "INSERT"
            
            logger.info(
                f"{operation} operation executed successfully in {duration:.2f}s\n"
                f"Object Path: {object_path}\n"
                f"Statement: {ddl_statement[:100]}{'...' if len(ddl_statement) > 100 else ''}"
            )
            return result
            
        except Exception as e:
            logger.error(
                f"DDL execution failed\n"
                f"Object Path: {object_path}\n"
                f"Statement: {ddl_statement[:100]}{'...' if len(ddl_statement) > 100 else ''}\n"
                f"Error: {str(e)}"
            )
            raise DDLExecutionError(f"DDL execution failed: {str(e)}")
    
    return wrapper

class SnowflakeService:
    """
    Snowflake service configuration and management.

    This class handles the configuration and setup of Snowflake Cortex services
    including search, analyst, and agent services. It loads service specifications
    from a YAML configuration file and provides access to service parameters.

    Parameters
    ----------
    account_identifier : str, optional
        Snowflake account identifier
    username : str, optional
        Snowflake username for authentication
    pat : str, optional
        Programmatic Access Token for Snowflake authentication
    config_path : str, optional
        Path to the service configuration YAML file
    warehouse : str, optional
        Default warehouse to use for operations

    Attributes
    ----------
    account_identifier : str
        Snowflake account identifier
    username : str
        Snowflake username
    pat : str
        Programmatic Access Token
    config_path : str
        Path to configuration file
    warehouse : str, optional
        Default warehouse to use for operations
    default_complete_model : str
        Default model for Cortex Complete operations
    search_services : list
        List of configured search service specifications
    analyst_services : list
        List of configured analyst service specifications
    agent_services : list
        List of configured agent service specifications
    """

    def __init__(
        self,
        account_identifier: Optional[str] = None,
        username: Optional[str] = None,
        pat: Optional[str] = None,
        config_path: Optional[str] = None,
        warehouse: Optional[str] = None,
    ):
        self.account_identifier = account_identifier
        self.username = username
        self.pat = pat
        self.config_path = config_path
        self.warehouse = warehouse
        self.default_complete_model = None
        self.search_services = []
        self.analyst_services = []
        self.agent_services = []
        self.unpack_service_specs()
        self.set_query_tag(
            major_version=tag_major_version, minor_version=tag_minor_version
        )

    def unpack_service_specs(self) -> None:
        """
        Load and parse service specifications from configuration file.

        Reads the YAML configuration file and extracts service specifications
        for search, analyst, and agent services. Also sets the default
        completion model.
        """
        try:
            # Load the service configuration from a YAML file
            with open(self.config_path, "r") as file:
                service_config = yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Service configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading service config: {e}")
            raise

        # Extract the service specifications
        try:
            self.search_services = service_config.get("search_services", [])
            self.analyst_services = service_config.get("analyst_services", [])
            self.agent_services = service_config.get(
                "agent_services", []
            )  # Not supported yet
            self.default_complete_model = service_config.get("cortex_complete", {}).get(
                "default_model", None
            )
        except Exception as e:
            logger.error(f"Error extracting service specifications: {e}")
            raise

        if self.default_complete_model is None:
            logger.warning(
                "No default model found in the service specification. Using snowflake-llama-3.3-70b as default."
            )

    def set_query_tag(
        self,
        query_tag: dict[str, str] = {"origin": "sf_sit", "name": "mcp_server"},
        major_version: Optional[int] = None,
        minor_version: Optional[int] = None,
    ) -> None:
        """
        Set the query tag for the Snowflake service.

        Parameters
        ----------
        query_tag : dict[str, str], optional
            Query tag dictionary
        major_version : int, optional
            Major version of the query tag
        minor_version : int, optional
            Minor version of the query tag
        """
        if major_version is not None and minor_version is not None:
            query_tag["version"] = {"major": major_version, "minor": minor_version}

        try:
            with (
                connect(
                    account=self.account_identifier,
                    user=self.username,
                    password=self.pat,
                    session_parameters={
                        "QUERY_TAG": json.dumps(query_tag),
                    },
                ) as con,
                con.cursor() as cur,
            ):
                cur.execute("SELECT 1").fetchone()
        except Exception as e:
            logger.warning(f"Error setting query tag: {e}")


async def load_service_config_resource(file_path: str) -> str:
    """
    Load service configuration from YAML file as JSON string.

    Parameters
    ----------
    file_path : str
        Path to the YAML configuration file

    Returns
    -------
    str
        JSON string representation of the configuration

    Raises
    ------
    FileNotFoundError
        If the configuration file cannot be found
    yaml.YAMLError
        If the YAML file is malformed
    """
    with open(file_path, "r") as file:
        service_config = yaml.safe_load(file)

    return json.dumps(service_config)


async def main(account_identifier: str, username: str, pat: str, config_path: str, warehouse: Optional[str] = None):
    """
    Main server setup and execution function.

    Initializes the Snowflake MCP server with the provided credentials and
    configuration. Sets up resource handlers, tool handlers, and starts
    the server using stdio streams.

    Parameters
    ----------
    account_identifier : str
        Snowflake account identifier
    username : str
        Snowflake username for authentication
    pat : str
        Programmatic Access Token for Snowflake authentication
    config_path : str
        Path to the service configuration YAML file
    warehouse : str, optional
        Default warehouse to use for operations

    Raises
    ------
    ValueError
        If required parameters are missing or invalid
    ConnectionError
        If unable to connect to Snowflake services
    """
    snowflake_service = SnowflakeService(
        account_identifier=account_identifier,
        username=username,
        pat=pat,
        config_path=config_path,
        warehouse=warehouse,
    )
    server = Server("snowflake")

    # For DEBUGGING
    logger.info("Starting Snowflake MCP server")

    @server.list_resources()
    async def list_resources() -> list[types.Resource]:
        """
        List available resources.

        Returns
        -------
        list[types.Resource]
            List of available resources including service configuration
        """
        return [
            types.Resource(
                uri=config_file_uri.as_uri(),
                name="Service Specification Configuration",
                description="Service Specification Configuration",
                mimeType="application/yaml",
            )
        ]

    @server.read_resource()
    async def read_resource(uri: AnyUrl) -> str:
        """
        Read resource content by URI.

        Parameters
        ----------
        uri : AnyUrl
            URI of the resource to read

        Returns
        -------
        str
            Resource content as string

        Raises
        ------
        ValueError
            If the requested resource URI is not found
        """
        if str(uri) == config_file_uri.as_uri():
            service_config = await load_service_config_resource(
                snowflake_service.config_path
            )

            return service_config

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        """
        List available tools.

        Returns all available tools including base tools (complete, models,
        specification) and dynamically generated tools from service
        configurations (search and analyst services).

        Returns
        -------
        list[types.Tool]
            List of all available tools
        """
        # Define tool types for Cortex Search Service
        search_tools_types = tools.get_cortex_search_tool_types(
            snowflake_service.search_services
        )
        # Define tool types for Cortex Analyst Service
        analyst_tools_types = tools.get_cortex_analyst_tool_types(
            snowflake_service.analyst_services
        )
        # Tools that are not dynamically instantiated based on config file
        base_tools = [
            # Cortex Complete Tool Type
            tools.get_cortex_complete_tool_type(),
            # Get model cards
            tools.get_cortex_models_tool_type(),
            # Get spec config file
            types.Tool(
                name="get-specification-resource",
                description="""Retrieves the service specification resource""",
                inputSchema={"type": "object", "properties": {}, "required": []},
            ),
            # DDL execution tool
            types.Tool(
                name="execute_snowflake_ddl",
                description="Executes DDL statements in Snowflake to create or modify objects. Supports CREATE, ALTER, DROP, UPDATE, and INSERT operations.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "ddl_statement": {
                            "type": "string",
                            "description": "The DDL statement to execute"
                        },
                        "object_path": {
                            "type": "string",
                            "description": "Optional fully qualified object path (database.schema.object) to specify the context",
                            "required": False
                        },
                        "warehouse": {
                            "type": "string",
                            "description": "Optional warehouse to use for this operation",
                            "required": False
                        }
                    },
                    "required": ["ddl_statement"]
                }
            ),
        ]

        return base_tools + search_tools_types + analyst_tools_types

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """
        Handle tool execution requests.

        Routes tool calls to appropriate handlers based on tool name.
        Supports specification retrieval, model management, completion,
        search, analyst tools, and DDL execution.

        Parameters
        ----------
        name : str
            Name of the tool to execute
        arguments : dict, optional
            Tool-specific arguments

        Returns
        -------
        list[types.TextContent | types.ImageContent | types.EmbeddedResource]
            Tool execution results

        Raises
        ------
        ValueError
            If required parameters are missing or tool is not found
        """
        if name == "get-specification-resource":
            spec = await read_resource(config_file_uri.as_uri())
            return [
                types.EmbeddedResource(
                    type="resource",
                    resource=types.TextResourceContents(
                        text=spec,
                        uri=config_file_uri.as_uri(),
                        mimeType="application/json",
                    ),
                )
            ]

        if name == "get-model-cards":
            # Call the cortex_complete function
            response = await tools.get_cortex_models(
                account_identifier=snowflake_service.account_identifier,
                username=snowflake_service.username,
                PAT=snowflake_service.pat,
            )

            if response:
                return [types.TextContent(type="text", text=json.dumps(response))]
            else:
                raise ValueError("No model cards found.")

        if name == "cortex-complete":
            # Validate required parameters
            prompt = arguments.get("prompt")
            if not prompt:
                raise ValueError("Missing required parameters")

            model = arguments.get("model")
            if not model:
                model = snowflake_service.default_complete_model

            response_format = arguments.get("response_format")

            # Call the cortex_complete function
            response = await tools.cortex_complete(
                prompt=prompt,
                model=model,
                account_identifier=snowflake_service.account_identifier,
                PAT=snowflake_service.pat,
                response_format=response_format,
            )

            return [types.TextContent(type="text", text=str(response))]

        if name == "execute_snowflake_ddl":
            return await tools.execute_snowflake_ddl(
                ddl_statement=arguments["ddl_statement"],
                object_path=arguments.get("object_path"),
                warehouse=arguments.get("warehouse", snowflake_service.warehouse),
                account_identifier=snowflake_service.account_identifier,
                username=snowflake_service.username,
                PAT=snowflake_service.pat,
            )

        if name in [
            spec.get("service_name") for spec in snowflake_service.search_services
        ]:
            # Find the corresponding service specification
            service_spec = next(
                (
                    spec
                    for spec in snowflake_service.search_services
                    if spec.get("service_name") == name
                ),
                None,
            )
            if not service_spec:
                raise ValueError(f"Service specification for {name} not found")

            # Extract parameters from the service specification
            database_name = service_spec.get("database_name")
            schema_name = service_spec.get("schema_name")

            # Validate required parameters
            query = arguments.get("query")
            columns = arguments.get("columns", [])
            filter_query = arguments.get("filter_query", None)
            if not query:
                raise ValueError("Missing required parameters")

            # Call the query_cortex_search function
            response = await tools.query_cortex_search(
                account_identifier=snowflake_service.account_identifier,
                service_name=name,
                database_name=database_name,
                schema_name=schema_name,
                query=query,
                PAT=snowflake_service.pat,
                columns=columns,
                filter_query=filter_query,
            )

            return [types.TextContent(type="text", text=str(response))]

        if name in [
            spec.get("service_name") for spec in snowflake_service.analyst_services
        ]:
            # Find the corresponding service specification
            service_spec = next(
                (
                    spec
                    for spec in snowflake_service.analyst_services
                    if spec.get("service_name") == name
                ),
                None,
            )
            if not service_spec:
                raise ValueError(f"Service specification for {name} not found")

            # Extract parameters from the service specification
            semantic_model = service_spec.get("semantic_model")

            # Validate required parameters
            query = arguments.get("query")
            if not query:
                raise ValueError("Missing required parameters")

            # Call the query_cortex_analyst function
            response = await tools.query_cortex_analyst(
                account_identifier=snowflake_service.account_identifier,
                semantic_model=semantic_model,
                query=query,
                username=snowflake_service.username,
                PAT=snowflake_service.pat,
            )

            return [types.TextContent(type="text", text=str(response))]

        raise ValueError(f"Unknown tool: {name}")

    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name=server_name,
                server_version=server_version,
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


