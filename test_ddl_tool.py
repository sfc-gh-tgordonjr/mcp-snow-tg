import asyncio
import json
import sys
sys.path.append('.')

from mcp.client import Client

async def test_ddl_tool():
    # Connect to the MCP server
    client = Client()
    await client.connect()
    
    # Make the tool call
    response = await client.call_tool(
        name="execute_snowflake_ddl",
        arguments={
            "ddl_statement": "CREATE DATABASE IF NOT EXISTS CURSOR_AGENT_TEST;"
        }
    )
    
    print("Response from execute_snowflake_ddl:")
    print(json.dumps(response, indent=2))
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(test_ddl_tool()) 