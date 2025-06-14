#!/usr/bin/env python3
"""
Debug script to test MCP server tools directly
"""
import asyncio
import json
from mcp_server_snowflake.server import main, SnowflakeService
from mcp_server_snowflake import tools

async def test_basic_functionality():
    """Test basic MCP server functionality"""
    
    # Test service initialization
    try:
        service = SnowflakeService(
            account_identifier="SFSENORTHAMERICA-TGORDON-AWS1",
            username="TGORDONJR", 
            pat="eyJraWQiOiIzNzQ2ODczNjM1MjEzMzQyIiwiYWxnIjoiRVMyNTYifQ.eyJwIjoiMjIzMzMxMTU2OjU3MTcyNzU0OTQ5IiwiaXNzIjoiU0Y6MTA0MyIsImV4cCI6MTc1NjUyNjc1Mn0.-DBqppb6vtma6pZqUApUWTN2lGX9PEa1EWz9A4D_uH0CRlL2T2tj_08Rggh6Xa1Ph7F2VDymYtErJVwF9cCJ6g",
            config_path="services/service_config.yaml",
            warehouse="CURSOR_AGENT"
        )
        print("✅ Service initialization successful")
        print(f"Default model: {service.default_complete_model}")
        print(f"Search services: {len(service.search_services)}")
        print(f"Analyst services: {len(service.analyst_services)}")
        
    except Exception as e:
        print(f"❌ Service initialization failed: {e}")
        return
    
    # Test get_cortex_models
    try:
        print("\n🔍 Testing get_cortex_models...")
        models = await tools.get_cortex_models(
            account_identifier="SFSENORTHAMERICA-TGORDON-AWS1",
            username="TGORDONJR",
            PAT="eyJraWQiOiIzNzQ2ODczNjM1MjEzMzQyIiwiYWxnIjoiRVMyNTYifQ.eyJwIjoiMjIzMzMxMTU2OjU3MTcyNzU0OTQ5IiwiaXNzIjoiU0Y6MTA0MyIsImV4cCI6MTc1NjUyNjc1Mn0.-DBqppb6vtma6pZqUApUWTN2lGX9PEa1EWz9A4D_uH0CRlL2T2tj_08Rggh6Xa1Ph7F2VDymYtErJVwF9cCJ6g"
        )
        print(f"✅ Models retrieved: {len(models) if models else 0}")
        if models:
            print(f"First model: {list(models.keys())[0] if isinstance(models, dict) else models[0]}")
    except Exception as e:
        print(f"❌ get_cortex_models failed: {e}")
    
    # Test cortex_complete with a simple prompt
    try:
        print("\n🤖 Testing cortex_complete...")
        response = await tools.cortex_complete(
            prompt="Hello, how are you?",
            model="mistral-large2",
            account_identifier="SFSENORTHAMERICA-TGORDON-AWS1",
            PAT="eyJraWQiOiIzNzQ2ODczNjM1MjEzMzQyIiwiYWxnIjoiRVMyNTYifQ.eyJwIjoiMjIzMzMxMTU2OjU3MTcyNzU0OTQ5IiwiaXNzIjoiU0Y6MTA0MyIsImV4cCI6MTc1NjUyNjc1Mn0.-DBqppb6vtma6pZqUApUWTN2lGX9PEa1EWz9A4D_uH0CRlL2T2tj_08Rggh6Xa1Ph7F2VDymYtErJVwF9cCJ6g"
        )
        print(f"✅ Cortex complete successful")
        print(f"Response type: {type(response)}")
        if isinstance(response, str):
            try:
                parsed = json.loads(response)
                print(f"Response preview: {str(parsed)[:200]}...")
            except:
                print(f"Response preview: {response[:200]}...")
    except Exception as e:
        print(f"❌ cortex_complete failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_basic_functionality()) 