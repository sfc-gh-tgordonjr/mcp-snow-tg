from typing import Dict, List, Optional, Union
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
from .utils import get_snowflake_connection

class DDLManager:
    """A class to manage DDL operations in Snowflake."""
    
    def __init__(
        self,
        account_identifier: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection: Optional[snowflake.connector.SnowflakeConnection] = None,
        **kwargs
    ):
        """Initialize the DDL manager.
        
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
        
    def execute_ddl(self, ddl_statement: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Execute a DDL statement and return the result.
        
        Args:
            ddl_statement: The DDL statement to execute
            
        Returns:
            Dict containing:
                - success: Boolean indicating if the operation was successful
                - message: Status message
                - results: List of results if any were returned
        """
        try:
            cursor: SnowflakeCursor = self.connection.cursor()
            results = cursor.execute(ddl_statement).fetchall()
            
            return {
                "success": True,
                "message": "DDL operation executed successfully",
                "results": [str(row) for row in results] if results else []
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error executing DDL: {str(e)}",
                "results": []
            }
            
    def create_database(self, database_name: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Create a new database.
        
        Args:
            database_name: Name of the database to create
            
        Returns:
            Dict containing operation status
        """
        ddl = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        return self.execute_ddl(ddl)
        
    def create_schema(self, database_name: str, schema_name: str) -> Dict[str, Union[bool, str, List[str]]]:
        """Create a new schema in the specified database.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema to create
            
        Returns:
            Dict containing operation status
        """
        ddl = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
        return self.execute_ddl(ddl)
        
    def create_table(self, database_name: str, schema_name: str, table_name: str, 
                    columns: List[Dict[str, str]]) -> Dict[str, Union[bool, str, List[str]]]:
        """Create a new table with specified columns.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table to create
            columns: List of column definitions, each containing 'name' and 'type'
            
        Returns:
            Dict containing operation status
        """
        column_defs = [f"{col['name']} {col['type']}" for col in columns]
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
            {', '.join(column_defs)}
        )
        """
        return self.execute_ddl(ddl)
        
    def drop_object(self, object_type: str, object_name: str, 
                   cascade: bool = False) -> Dict[str, Union[bool, str, List[str]]]:
        """Drop a database object (database, schema, table, etc.).
        
        Args:
            object_type: Type of object to drop (DATABASE, SCHEMA, TABLE, etc.)
            object_name: Fully qualified name of the object
            cascade: Whether to cascade the drop operation
            
        Returns:
            Dict containing operation status
        """
        cascade_str = "CASCADE" if cascade else ""
        ddl = f"DROP {object_type} IF EXISTS {object_name} {cascade_str}"
        return self.execute_ddl(ddl) 