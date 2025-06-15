from typing import Dict, List, Optional, Union
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from mcp_server_snowflake.utils import format_schema_name, get_snowflake_connection

class SnowflakeOperations:
    """A class to manage non-DDL Snowflake operations."""
    
    def __init__(
        self,
        account_identifier: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection: Optional[snowflake.connector.SnowflakeConnection] = None,
        **kwargs
    ):
        """Initialize the Snowflake operations manager.
        
        Args:
            account_identifier: Snowflake account identifier
            username: Snowflake username
            password: Snowflake password or PAT
            connection: Optional existing Snowflake connection
            **kwargs: Additional connection parameters
        """
        self.connection = connection or get_snowflake_connection(
            account_identifier=account_identifier,
            username=username,
            password=password,
            **kwargs
        )
        
    def execute_query(self, query: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Execute a Snowflake query and return the result.
        
        Args:
            query: The query to execute
            
        Returns:
            Dict containing:
                - success: Boolean indicating if the operation was successful
                - message: Status message
                - results: List of results if any were returned
        """
        try:
            cursor: SnowflakeCursor = self.connection.cursor()
            results = cursor.execute(query).fetchall()
            
            return {
                "success": True,
                "message": "Query executed successfully",
                "results": [str(row) for row in results] if results else []
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error executing query: {str(e)}",
                "results": []
            }
            
    def show_objects(
        self,
        object_type: str,
        pattern: Optional[str] = None
    ) -> Dict[str, Union[bool, str, List[str]]]:
        """Show Snowflake objects of a specific type.
        
        Args:
            object_type: Type of objects to show (DATABASES, SCHEMAS, TABLES, etc.)
            pattern: Optional pattern to filter objects by name
            
        Returns:
            Dict containing operation status and results
        """
        query = f"SHOW {object_type}"
        if pattern:
            query += f" LIKE '{pattern}'"
        return self.execute_query(query)
        
    def describe_object(self, object_name: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Describe a Snowflake object.
        
        Args:
            object_name: Fully qualified name of the object to describe
            
        Returns:
            Dict containing operation status and results
        """
        query = f"DESCRIBE {object_name}"
        return self.execute_query(query)
        
    def use_context(self, context_type: str, context_name: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Set the current context (database, schema, warehouse, role).
        
        Args:
            context_type: Type of context to set (DATABASE, SCHEMA, WAREHOUSE, ROLE)
            context_name: Name of the context to use
            
        Returns:
            Dict containing operation status
        """
        # If setting schema context, ensure it's properly formatted
        if context_type.upper() == "SCHEMA":
            context_name = format_schema_name(context_name)
            
        query = f"USE {context_type} {context_name}"
        return self.execute_query(query)
        
    def grant_privilege(
        self,
        privileges: Union[str, List[str]],
        on_type: str,
        on_name: str,
        to_type: str,
        to_name: str
    ) -> Dict[str, Union[bool, str, List[str]]]:
        """Grant privileges to a role or user.
        
        Args:
            privileges: Single privilege or list of privileges to grant
            on_type: Type of object to grant privileges on (DATABASE, SCHEMA, TABLE, etc.)
            on_name: Name of the object to grant privileges on
            to_type: Type of grantee (ROLE, USER)
            to_name: Name of the role or user to grant privileges to
            
        Returns:
            Dict containing operation status
        """
        # If granting on schema, ensure it's properly formatted
        if on_type.upper() == "SCHEMA":
            on_name = format_schema_name(on_name)
            
        if isinstance(privileges, list):
            privileges_str = ", ".join(privileges)
        else:
            privileges_str = privileges
            
        query = f"GRANT {privileges_str} ON {on_type} {on_name} TO {to_type} {to_name}"
        return self.execute_query(query)
        
    def revoke_privilege(
        self,
        privileges: Union[str, List[str]],
        on_type: str,
        on_name: str,
        from_type: str,
        from_name: str
    ) -> Dict[str, Union[bool, str, List[str]]]:
        """Revoke privileges from a role or user.
        
        Args:
            privileges: Single privilege or list of privileges to revoke
            on_type: Type of object to revoke privileges from (DATABASE, SCHEMA, TABLE, etc.)
            on_name: Name of the object to revoke privileges from
            from_type: Type of grantee (ROLE, USER)
            from_name: Name of the role or user to revoke privileges from
            
        Returns:
            Dict containing operation status
        """
        # If revoking from schema, ensure it's properly formatted
        if on_type.upper() == "SCHEMA":
            on_name = format_schema_name(on_name)
            
        if isinstance(privileges, list):
            privileges_str = ", ".join(privileges)
        else:
            privileges_str = privileges
            
        query = f"REVOKE {privileges_str} ON {on_type} {on_name} FROM {from_type} {from_name}"
        return self.execute_query(query)
        
    def alter_warehouse(
        self,
        warehouse_name: str,
        size: Optional[str] = None,
        min_cluster_count: Optional[int] = None,
        max_cluster_count: Optional[int] = None,
        scaling_policy: Optional[str] = None,
        auto_suspend: Optional[int] = None,
        auto_resume: Optional[bool] = None,
        enable_query_acceleration: Optional[bool] = None
    ) -> Dict[str, Union[bool, str, List[str]]]:
        """Alter warehouse settings.
        
        Args:
            warehouse_name: Name of the warehouse to alter
            size: Warehouse size (XSMALL, SMALL, MEDIUM, LARGE, XLARGE, etc.)
            min_cluster_count: Minimum number of clusters
            max_cluster_count: Maximum number of clusters
            scaling_policy: Scaling policy (STANDARD or ECONOMY)
            auto_suspend: Number of seconds of inactivity before suspending
            auto_resume: Whether to auto-resume when queries are submitted
            enable_query_acceleration: Whether to enable query acceleration
            
        Returns:
            Dict containing operation status
        """
        alter_clauses = []
        
        if size:
            alter_clauses.append(f"WAREHOUSE_SIZE = {size}")
            
        if min_cluster_count is not None:
            alter_clauses.append(f"MIN_CLUSTER_COUNT = {min_cluster_count}")
            
        if max_cluster_count is not None:
            alter_clauses.append(f"MAX_CLUSTER_COUNT = {max_cluster_count}")
            
        if scaling_policy:
            alter_clauses.append(f"SCALING_POLICY = {scaling_policy}")
            
        if auto_suspend is not None:
            alter_clauses.append(f"AUTO_SUSPEND = {auto_suspend}")
            
        if auto_resume is not None:
            alter_clauses.append(f"AUTO_RESUME = {'TRUE' if auto_resume else 'FALSE'}")
            
        if enable_query_acceleration is not None:
            alter_clauses.append(f"ENABLE_QUERY_ACCELERATION = {'TRUE' if enable_query_acceleration else 'FALSE'}")
            
        if not alter_clauses:
            return {
                "success": False,
                "message": "No warehouse properties specified to alter",
                "results": []
            }
            
        query = f"ALTER WAREHOUSE {warehouse_name} SET {', '.join(alter_clauses)}"
        return self.execute_query(query) 