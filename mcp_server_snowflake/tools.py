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
import requests
from typing import Optional
from collections import OrderedDict

import mcp.types as types
from bs4 import BeautifulSoup
from snowflake.connector import DictCursor
from snowflake.connector import connect

from mcp_server_snowflake.utils import SnowflakeResponse, SnowflakeException
from .helpers.ddl_manager import DDLManager
from .helpers.dml_manager import DMLManager
from .helpers.snowflake_operations import SnowflakeOperations


sfse = SnowflakeResponse()  # For parsing Snowflake responses


# Cortex Search Service
@sfse.snowflake_response(api="search")
async def query_cortex_search(
    account_identifier: str,
    service_name: str,
    database_name: str,
    schema_name: str,
    query: str,
    PAT: str,
    columns: Optional[list[str]] = None,
    filter_query: Optional[dict] = {},
) -> dict:
    """
    Query a Cortex Search Service using the REST API.

    Performs semantic search against a configured Cortex Search service using
    Snowflake's REST API. Supports filtering and column selection for refined
    search results.

    Parameters
    ----------
    account_identifier : str
        Snowflake account identifier
    service_name : str
        Name of the Cortex Search Service
    database_name : str
        Target database containing the search service
    schema_name : str
        Target schema containing the search service
    query : str
        The search query string to submit to Cortex Search
    PAT : str
        Programmatic Access Token for authentication
    columns : list[str], optional
        List of columns to return for each relevant result, by default None
    filter_query : dict, optional
        Filter query to apply to search results, by default {}

    Returns
    -------
    dict
        JSON response from the Cortex Search API containing search results

    Raises
    ------
    SnowflakeException
        If the API request fails or returns an error status code

    References
    ----------
    Snowflake Cortex Search REST API:
    https://docs.snowflake.com/developer-guide/snowflake-rest-api/reference/cortex-search-service
    """
    base_url = f"https://{account_identifier}.snowflakecomputing.com/api/v2/databases/{database_name}/schemas/{schema_name}/cortex-search-services/{service_name}:query"

    headers = {
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        "Authorization": f"Bearer {PAT}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    payload = {
        "query": query,
        "filter": filter_query,
    }

    if isinstance(columns, list) and len(columns) > 0:
        payload["columns"] = columns

    response = requests.post(base_url, headers=headers, json=payload)

    if response.status_code == 200:
        return response
    else:
        raise SnowflakeException(
            tool="Cortex Search",
            status_code=response.status_code,
            message=response.text,
        )


def get_cortex_search_tool_types(search_services: list[dict]) -> list[types.Tool]:
    """
    Generate MCP tool definitions for configured search services.

    Creates tool specifications for each configured Cortex Search service,
    including input schemas with query parameters, column selection, and
    filtering options.

    Parameters
    ----------
    search_services : list[dict]
        List of search service configuration dictionaries containing
        service_name, description, and other service metadata

    Returns
    -------
    list[types.Tool]
        List of MCP Tool objects with complete input schemas for search operations

    Notes
    -----
    The generated tools support advanced filtering with operators:
    - @eq: Equality matching for text/numeric values
    - @contains: Array contains matching
    - @gte/@lte: Numeric/date range filtering
    - @and/@or/@not: Logical operators for complex filters
    """

    return [
        types.Tool(
            name=x.get("service_name"),
            description=x.get("description"),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "User query to search in search service",
                    },
                    "columns": {
                        "type": "array",
                        "description": "Optional list of columns to return for each relevant result in the response.",
                    },
                    "filter_query": {
                        "type": "object",
                        "description": """Cortex Search supports filtering on the ATTRIBUTES columns specified in the CREATE CORTEX SEARCH SERVICE command.

                        Cortex Search supports four matching operators:

                        1. TEXT or NUMERIC equality: @eq
                        2. ARRAY contains: @contains
                        3. NUMERIC or DATE/TIMESTAMP greater than or equal to: @gte
                        4. NUMERIC or DATE/TIMESTAMP less than or equal to: @lte

                        These matching operators can be composed with various logical operators:

                        - @and
                        - @or
                        - @not

                        The following usage notes apply:

                        Matching against NaN ('not a number') values in the source query are handled as
                        described in Special values. Fixed-point numeric values with more than 19 digits (not
                        including leading zeroes) do not work with @eq, @gte, or @lte and will not be returned
                        by these operators (although they could still be returned by the overall query with the
                        use of @not).

                        TIMESTAMP and DATE filters accept values of the form: YYYY-MM-DD and, for timezone
                        aware dates: YYYY-MM-DD+HH:MM. If the timezone offset is not specified, the date is
                        interpreted in UTC.

                        These operators can be combined into a single filter object.

                        Example:
                        Filtering on rows where NUMERIC column numeric_col is between 10.5 and 12.5 (inclusive):

                        { "@and": [
                        { "@gte": { "numeric_col": 10.5 } },
                        { "@lte": { "numeric_col": 12.5 } }
                        ]}""",
                    },
                },
                "required": ["query"],
            },
        )
        for x in search_services
    ]


# Cortex Complete Service
@sfse.snowflake_response(api="complete")
async def cortex_complete(
    prompt: str,
    model: str,
    account_identifier: str,
    PAT: str,
    response_format: Optional[dict] = None,
) -> dict:
    """
    Generate text completions using Snowflake Cortex Complete API.

    Sends a chat completion request to Snowflake's Cortex Complete service
    using the specified language model. Supports structured JSON responses
    when a response format is provided.

    Parameters
    ----------
    prompt : str
        User prompt message to send to the language model
    model : str
        Snowflake Cortex LLM model name to use for completion
    account_identifier : str
        Snowflake account identifier
    PAT : str
        Programmatic Access Token for authentication
    response_format : dict, optional
        JSON schema for structured response format, by default None

    Returns
    -------
    dict
        JSON response from the Cortex Complete API containing the generated text

    Raises
    ------
    SnowflakeException
        If the API request fails or returns an error status code

    Notes
    -----
    The temperature is set to 0.0 for deterministic responses. The response_format
    parameter allows for structured JSON outputs following a provided schema.
    """
    base_url = f"https://{account_identifier}.snowflakecomputing.com/api/v2/cortex/inference:complete"

    headers = {
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        "Authorization": f"Bearer {PAT}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
    }

    # Add response_format to payload if provided
    if response_format is not None:
        payload["response_format"] = response_format

    response = requests.post(base_url, headers=headers, json=payload)

    if response.status_code == 200:
        return response
    else:
        raise SnowflakeException(
            tool="Cortex Complete",
            status_code=response.status_code,
            message=response.text,
        )


def get_cortex_complete_tool_type():
    """
    Generate MCP tool definition for Cortex Complete service.

    Creates a tool specification for the Cortex Complete LLM service with
    support for prompt input, model selection, and structured JSON responses.

    Returns
    -------
    types.Tool
        MCP Tool object with complete input schema for LLM completion operations

    Notes
    -----
    The tool supports optional structured JSON responses through the response_format
    parameter, which accepts a JSON schema defining the expected output structure.
    """
    return types.Tool(
        name="cortex-complete",
        description="""Simple LLM chat completion API using Cortex Complete""",
        inputSchema={
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "User prompt message to send to the LLM",
                },
                "model": {
                    "type": "string",
                    "description": "Optional Snowflake Cortex LLM Model name to use.",
                },
                "response_format": {
                    "type": "object",
                    "description": """Optional JSON response format to use for the LLM response.
                            Type must be 'json' and schema must be a valid JSON schema.
                            Example:
                            {
                                "type": "json",
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                    "people": {
                                        "type": "array",
                                        "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {
                                            "type": "string"
                                            },
                                            "age": {
                                            "type": "number"
                                            }
                                        },
                                        "required": ["name", "age"]
                                        }
                                    }
                                    },
                                    "required": ["people"]
                                }
                            }
                            """,
                },
            },
            "required": ["prompt"],
        },
    )


def get_region(
    account_identifier: str,
    username: str,
    PAT: str,
) -> str:
    """
    Retrieve the current region of the Snowflake account.

    Executes a SQL query to determine the region where the Snowflake
    account is located using the CURRENT_REGION() function.

    Parameters
    ----------
    account_identifier : str
        Snowflake account identifier
    username : str
        Snowflake username for authentication
    PAT : str
        Programmatic Access Token for authentication

    Returns
    -------
    str
        The region name where the Snowflake account is located

    Raises
    ------
    snowflake.connector.errors.Error
        If connection to Snowflake fails or query execution fails
    """

    statement = "SELECT CURRENT_REGION()"
    with (
        connect(
            account=account_identifier,
            user=username,
            password=PAT,
        ) as con,
        con.cursor(DictCursor) as cur,
    ):
        cur.execute(statement)
        return cur.fetchone().get("CURRENT_REGION()")


async def get_cortex_models(
    account_identifier: str,
    username: str,
    PAT: str,
    url: str = "https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-rest-api#model-availability",
) -> str | dict[str, list[dict[str, str]] | str]:
    """
    Retrieve available Cortex Complete model information from Snowflake documentation.

    Scrapes the Snowflake documentation to get current model availability
    information specifically for the REST API and combines it with the account's region
    information.

    Parameters
    ----------
    account_identifier : str
        Snowflake account identifier
    username : str
        Snowflake username for authentication
    PAT : str
        Programmatic Access Token for authentication
    url : str, optional
        URL to Snowflake Cortex model documentation, by default official docs URL

    Returns
    -------
    str | dict[str, list[dict[str, str]] | str]
        Either an error message string or a dictionary containing:
        - 'current_region': The account's region
        - 'model_availability': List of available models with their details
    """

    # Send HTTP request
    response = requests.get(url)
    if response.status_code != 200:
        return f"Failed to retrieve the page {url} with {response.status_code}"

    # Parse HTML
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the model availability section (could be a table or other format)
    section = soup.find(id="model-availability") or soup.find(
        string="Model availability"
    ).find_parent("section")

    if not section:
        return (
            f"Failed to retrieve model availability from the docs. Please visit {url}."
        )

    else:
        # Process the specific section if found
        tables = section.find_all("table")
        if tables:
            model_data = []
            table = tables[0]

            # Get headers
            headers = []
            for th in table.find_all("th"):
                headers.append(th.text.strip())

            # Extract rows
            for row in table.find_all("tr")[1:]:  # Skip header row
                cells = row.find_all(["td", "th"])
                if cells:
                    row_data = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            row_data[headers[i]] = cell.text.strip()
                    model_data.append(row_data)

            return OrderedDict(
                [
                    ("current_region", get_region(account_identifier, username, PAT)),
                    ("model_availability", model_data),
                ]
            )
        else:
            return f"No model availability table found at {url}."


def get_cortex_models_tool_type():
    """
    Generate MCP tool definition for retrieving Cortex model information.

    Creates a tool specification for fetching available Cortex Complete
    models and their regional availability.

    Returns
    -------
    types.Tool
        MCP Tool object for retrieving model cards and availability information
    """
    return types.Tool(
        name="get-model-cards",
        description="""Retrieves available model cards in Snowflake Cortex REST API""",
        inputSchema={"type": "object", "properties": {}, "required": []},
    )


# Cortex Analyst Service
@sfse.snowflake_response(api="analyst")
async def query_cortex_analyst(
    account_identifier: str,
    semantic_model: str,
    query: str,
    username: str,
    PAT: str,
) -> dict:
    """
    Query Snowflake Cortex Analyst service for natural language to SQL conversion.

    Sends a natural language query to the Cortex Analyst service, which
    interprets the query against a semantic model and generates appropriate
    SQL responses with explanations.

    Parameters
    ----------
    account_identifier : str
        Snowflake account identifier
    semantic_model : str
        Fully qualified path to YAML semantic file or Snowflake Semantic View.
        Examples:
        - "@my_db.my_schema.my_stage/my_semantic_model.yaml"
        - "MY_DB.MY_SCH.MY_SEMANTIC_VIEW"
    query : str
        Natural language query string to submit to Cortex Analyst
    username : str
        Snowflake username for authentication
    PAT : str
        Programmatic Access Token for authentication

    Returns
    -------
    dict
        JSON response from the Cortex Analyst API containing generated SQL,
        explanations, and query results

    Raises
    ------
    SnowflakeException
        If the API request fails or returns an error status code

    Notes
    -----
    The function automatically detects whether the semantic_model parameter
    refers to a YAML file (starts with @ and ends with .yaml) or a semantic view.
    Currently configured for non-streaming responses.
    """
    base_url = f"https://{account_identifier}.snowflakecomputing.com/api/v2/cortex/analyst/message"

    headers = {
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        "Authorization": f"Bearer {PAT}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    if semantic_model.startswith("@") and semantic_model.endswith(".yaml"):
        semantic_type = "semantic_model_file"
    else:
        semantic_type = "semantic_model_view"

    payload = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": query,
                    }
                ],
            }
        ],
        semantic_type: semantic_model,
        "stream": False,
    }

    response = requests.post(base_url, headers=headers, json=payload)

    if response.status_code == 200:
        return response

    else:
        raise SnowflakeException(
            tool="Cortex Analyst",
            status_code=response.status_code,
            message=response.text,
        )


def get_cortex_analyst_tool_types(analyst_services: list[dict]) -> list[types.Tool]:
    """
    Generate MCP tool definitions for configured Cortex Analyst services.

    Creates tool specifications for each configured Cortex Analyst service,
    enabling natural language querying against semantic models.

    Parameters
    ----------
    analyst_services : list[dict]
        List of analyst service configuration dictionaries containing
        service_name, description, and semantic model references

    Returns
    -------
    list[types.Tool]
        List of MCP Tool objects with input schemas for natural language queries
    """

    return [
        types.Tool(
            name=x.get("service_name"),
            description=x.get("description"),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A rephrased natural language prompt from the user.",
                    },
                },
                "required": ["query"],
            },
        )
        for x in analyst_services
    ]


async def execute_ddl_operation(
    operation: str,
    operation_type: str,
    parameters: dict,
    account_identifier: str,
    username: str,
    pat: str,
) -> dict:
    """Execute a DDL operation in Snowflake."""
    try:
        with connect(
            account=account_identifier,
            user=username,
            password=pat
        ) as conn:
            cursor = conn.cursor()
            
            if operation == "CREATE_DATABASE":
                ddl = f"CREATE DATABASE IF NOT EXISTS {parameters['database_name']}"
            elif operation == "CREATE_SCHEMA":
                ddl = f"CREATE SCHEMA IF NOT EXISTS {parameters['database_name']}.{parameters['schema_name']}"
            elif operation == "CREATE_TABLE":
                column_defs = [f"{col['name']} {col['type']}" for col in parameters['columns']]
                ddl = f"""
                CREATE TABLE IF NOT EXISTS {parameters['database_name']}.{parameters['schema_name']}.{parameters['table_name']} (
                    {', '.join(column_defs)}
                )
                """
            elif operation == "DROP":
                cascade = "CASCADE" if parameters.get("cascade", False) else ""
                ddl = f"DROP {operation_type} IF EXISTS {parameters['object_name']} {cascade}"
            elif operation == "EXECUTE":
                ddl = parameters["ddl_statement"]
            else:
                raise ValueError(f"Unsupported operation: {operation}")
            
            cursor.execute(ddl)
            results = cursor.fetchall()
            
            return {
                "success": True,
                "message": f"{operation} operation executed successfully",
                "results": [str(row) for row in results] if results else []
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "results": []
        }

def get_ddl_tool_type() -> types.Tool:
    """
    Generate the DDL tool definition.
    
    Returns
    -------
    types.Tool
        Tool specification for DDL operations
    """
    return types.Tool(
        name="DDL_MANAGER",
        description="Execute DDL operations in Snowflake",
        inputSchema={
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "description": "The DDL operation to perform",
                    "enum": ["CREATE_DATABASE", "CREATE_SCHEMA", "CREATE_TABLE", "DROP", "EXECUTE", "ALTER"]
                },
                "operation_type": {
                    "type": "string",
                    "description": "The type of object for the operation",
                    "enum": ["DATABASE", "SCHEMA", "TABLE", "VIEW", "PROCEDURE", "FUNCTION", "COLUMN"]
                },
                "parameters": {
                    "type": "object",
                    "description": "Operation-specific parameters",
                    "properties": {
                        "database_name": {"type": "string"},
                        "schema_name": {"type": "string"},
                        "table_name": {"type": "string"},
                        "object_name": {"type": "string"},
                        "cascade": {"type": "boolean"},
                        "ddl_statement": {"type": "string"},
                        "columns": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "type": {"type": "string"}
                                },
                                "required": ["name", "type"]
                            }
                        },
                        "alter_type": {
                            "type": "string",
                            "enum": ["ADD", "DROP", "RENAME", "ALTER"]
                        },
                        "column_name": {"type": "string"},
                        "new_name": {"type": "string"},
                        "data_type": {"type": "string"},
                        "default_value": {"type": "string"},
                        "not_null": {"type": "boolean"},
                        "new_database": {"type": "string"}
                    }
                }
            },
            "required": ["operation", "operation_type", "parameters"]
        }
    )

def get_snowflake_operations_tool_type() -> types.Tool:
    """
    Generate the Snowflake Operations tool definition.
    
    Returns
    -------
    types.Tool
        Tool specification for non-DDL Snowflake operations
    """
    return types.Tool(
        name="SNOWFLAKE_OPERATIONS",
        description="Execute non-DDL operations in Snowflake (SHOW, DESCRIBE, USE, GRANT, REVOKE)",
        inputSchema={
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "description": "The operation to perform",
                    "enum": ["SHOW", "DESCRIBE", "USE", "GRANT", "REVOKE", "ALTER_WAREHOUSE"]
                },
                "parameters": {
                    "type": "object",
                    "description": "Operation-specific parameters",
                    "properties": {
                        "object_type": {
                            "type": "string",
                            "description": "Type of object (for SHOW operations)",
                            "enum": ["DATABASES", "SCHEMAS", "TABLES", "VIEWS", "FUNCTIONS", "PROCEDURES", "WAREHOUSES", "ROLES"]
                        },
                        "pattern": {
                            "type": "string",
                            "description": "Pattern to filter objects (for SHOW operations)"
                        },
                        "object_name": {
                            "type": "string",
                            "description": "Name of the object to describe or use"
                        },
                        "context_type": {
                            "type": "string",
                            "description": "Type of context to use",
                            "enum": ["DATABASE", "SCHEMA", "WAREHOUSE", "ROLE"]
                        },
                        "context_name": {
                            "type": "string",
                            "description": "Name of the context to use"
                        },
                        "privileges": {
                            "type": "array",
                            "description": "List of privileges to grant/revoke",
                            "items": {"type": "string"}
                        },
                        "on_type": {
                            "type": "string",
                            "description": "Type of object to grant/revoke privileges on",
                            "enum": ["DATABASE", "SCHEMA", "TABLE", "VIEW", "FUNCTION", "PROCEDURE"]
                        },
                        "on_name": {
                            "type": "string",
                            "description": "Name of the object to grant/revoke privileges on"
                        },
                        "to_type": {
                            "type": "string",
                            "description": "Type of grantee",
                            "enum": ["ROLE", "USER"]
                        },
                        "to_name": {
                            "type": "string",
                            "description": "Name of the role/user to grant privileges to"
                        },
                        "from_type": {
                            "type": "string",
                            "description": "Type of grantee to revoke from",
                            "enum": ["ROLE", "USER"]
                        },
                        "from_name": {
                            "type": "string",
                            "description": "Name of the role/user to revoke privileges from"
                        },
                        "warehouse_name": {
                            "type": "string",
                            "description": "Name of the warehouse to alter"
                        },
                        "warehouse_size": {
                            "type": "string",
                            "description": "Size of the warehouse (XSMALL, SMALL, MEDIUM, LARGE, XLARGE, etc.)"
                        },
                        "min_cluster_count": {
                            "type": "integer",
                            "description": "Minimum number of clusters"
                        },
                        "max_cluster_count": {
                            "type": "integer",
                            "description": "Maximum number of clusters"
                        },
                        "scaling_policy": {
                            "type": "string",
                            "description": "Scaling policy (STANDARD or ECONOMY)"
                        },
                        "auto_suspend": {
                            "type": "integer",
                            "description": "Number of seconds of inactivity before suspending"
                        },
                        "auto_resume": {
                            "type": "boolean",
                            "description": "Whether to auto-resume when queries are submitted"
                        },
                        "enable_query_acceleration": {
                            "type": "boolean",
                            "description": "Whether to enable query acceleration"
                        }
                    }
                }
            },
            "required": ["operation", "parameters"]
        }
    )

async def execute_snowflake_operation(
    operation: str,
    parameters: dict,
    account_identifier: str,
    username: str,
    pat: str,
) -> dict:
    """Execute a non-DDL Snowflake operation."""
    ops = SnowflakeOperations(
        account_identifier=account_identifier,
        username=username,
        password=pat
    )
    
    try:
        if operation == "SHOW":
            return ops.show_objects(
                object_type=parameters["object_type"],
                pattern=parameters.get("pattern")
            )
            
        elif operation == "DESCRIBE":
            return ops.describe_object(
                object_name=parameters["object_name"]
            )
            
        elif operation == "USE":
            return ops.use_context(
                context_type=parameters["context_type"],
                context_name=parameters["context_name"]
            )
            
        elif operation == "GRANT":
            return ops.grant_privilege(
                privileges=parameters["privileges"],
                on_type=parameters["on_type"],
                on_name=parameters["on_name"],
                to_type=parameters["to_type"],
                to_name=parameters["to_name"]
            )
            
        elif operation == "REVOKE":
            return ops.revoke_privilege(
                privileges=parameters["privileges"],
                on_type=parameters["on_type"],
                on_name=parameters["on_name"],
                from_type=parameters["from_type"],
                from_name=parameters["from_name"]
            )
            
        elif operation == "ALTER_WAREHOUSE":
            return ops.alter_warehouse(
                warehouse_name=parameters["warehouse_name"],
                size=parameters.get("warehouse_size"),
                min_cluster_count=parameters.get("min_cluster_count"),
                max_cluster_count=parameters.get("max_cluster_count"),
                scaling_policy=parameters.get("scaling_policy"),
                auto_suspend=parameters.get("auto_suspend"),
                auto_resume=parameters.get("auto_resume"),
                enable_query_acceleration=parameters.get("enable_query_acceleration")
            )
            
        else:
            raise ValueError(f"Unsupported operation: {operation}")
            
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "results": []
        }

async def execute_dml_operation(
    operation: str,
    table_name: str,
    parameters: dict,
    account_identifier: str,
    username: str,
    pat: str,
) -> dict:
    """Execute a DML operation in Snowflake."""
    try:
        dml_manager = DMLManager(
            account_identifier=account_identifier,
            username=username,
            password=pat
        )
        
        if operation == "SELECT":
            return dml_manager.select_data(
                table_name=table_name,
                columns=parameters.get('columns'),
                where_clause=parameters.get('where_clause'),
                order_by=parameters.get('order_by'),
                limit=parameters.get('limit'),
                offset=parameters.get('offset')
            )
        elif operation == "INSERT":
            return dml_manager.insert_data(
                table_name=table_name,
                columns=parameters.get('columns', []),
                values=parameters.get('values', [])
            )
        elif operation == "UPDATE":
            return dml_manager.update_data(
                table_name=table_name,
                set_columns=parameters.get('set_columns', []),
                set_values=parameters.get('set_values', []),
                where_clause=parameters.get('where_clause', '')
            )
        elif operation == "DELETE":
            return dml_manager.delete_data(
                table_name=table_name,
                where_clause=parameters.get('where_clause', '')
            )
        elif operation == "MERGE":
            return dml_manager.merge_data(
                target_table=table_name,
                source_table=parameters.get('source_table', ''),
                merge_condition=parameters.get('merge_condition', ''),
                match_actions=parameters.get('match_actions', []),
                not_match_actions=parameters.get('not_match_actions', [])
            )
        else:
            raise ValueError(f"Unsupported operation: {operation}")
            
    except Exception as e:
        return {
            "success": False,
            "message": f"Error executing {operation}: {str(e)}",
            "results": [],
            "rows_affected": 0
        }

def get_dml_tool_type() -> types.Tool:
    """
    Generate the DML tool definition.
    
    Returns
    -------
    types.Tool
        Tool specification for DML operations
    """
    return types.Tool(
        name="DML_MANAGER",
        description="Execute DML operations in Snowflake",
        inputSchema={
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "description": "The DML operation to perform",
                    "enum": ["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"]
                },
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name (database.schema.table)"
                },
                "parameters": {
                    "type": "object",
                    "description": "Operation-specific parameters",
                    "properties": {
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Column names for SELECT/INSERT operation"
                        },
                        "where_clause": {
                            "type": "string",
                            "description": "WHERE clause for SELECT/UPDATE/DELETE operations"
                        },
                        "order_by": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Columns to order by for SELECT operation"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of rows to return for SELECT operation"
                        },
                        "offset": {
                            "type": "integer",
                            "description": "Number of rows to skip for SELECT operation"
                        },
                        "values": {
                            "type": "array",
                            "description": "Values to insert/update"
                        },
                        "set_columns": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Column names for UPDATE operation"
                        },
                        "set_values": {
                            "type": "array",
                            "description": "New values for UPDATE operation"
                        },
                        "source_table": {
                            "type": "string",
                            "description": "Source table or subquery for MERGE operation"
                        },
                        "merge_condition": {
                            "type": "string",
                            "description": "ON clause condition for MERGE operation"
                        },
                        "match_actions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "enum": ["UPDATE", "DELETE"]
                                    },
                                    "columns": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "values": {
                                        "type": "array"
                                    }
                                }
                            }
                        },
                        "not_match_actions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "action": {
                                        "type": "string",
                                        "enum": ["INSERT"]
                                    },
                                    "columns": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "values": {
                                        "type": "array"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "required": ["operation", "table_name", "parameters"]
        }
    )
