import asyncio
import httpx
import json
from context_fetcher import ContextFetcher


async def debug_context_extraction():
    """Debug what we're actually extracting from context files"""

    # Use the job_id from your GOLD file
    job_id = "2835d651-44ba-4a6f-b048-5d94500d5974"
    base_url = "https://assetmanagement-production-f542.up.railway.app"

    print(f"🔍 DEBUGGING WITH JOB_ID: {job_id}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for asset in ['NIFTY50', 'GOLD', 'BITCOIN', 'REIT']:
            print(f"\n{'=' * 50}")
            print(f"DEBUGGING {asset}")
            print(f"{'=' * 50}")

            try:
                # Method 1: Direct API call
                url = f"{base_url}/jobs/{job_id}/context/{asset}"
                print(f"🌐 Calling: {url}")

                response = await client.get(url)
                print(f"📊 Status: {response.status_code}")

                if response.status_code == 200:
                    try:
                        # Parse the response
                        response_data = response.json()
                        print(f"📋 Response keys: {list(response_data.keys())}")

                        # Check if we have context_data wrapper
                        if 'context_data' in response_data:
                            context_data = response_data['context_data']
                            print(f"📂 Using context_data wrapper")
                        else:
                            context_data = response_data
                            print(f"📂 Using direct response")

                        # Extract component breakdown
                        component_breakdown = context_data.get('component_breakdown', {})
                        print(f"🔍 Component breakdown keys: {list(component_breakdown.keys())}")

                        # Show first few components in detail
                        for i, (comp_name, comp_data) in enumerate(component_breakdown.items()):
                            if i < 3:  # Show first 3 components
                                print(f"  📌 {comp_name}:")
                                print(f"    sentiment: {comp_data.get('sentiment', 'MISSING')}")
                                print(f"    confidence: {comp_data.get('confidence', 'MISSING')}")
                                print(f"    contribution: {comp_data.get('contribution', 'MISSING')}")

                        # Test our extraction function
                        print(f"\n--- TESTING EXTRACTION FUNCTION ---")
                        fetcher = ContextFetcher()
                        extracted = fetcher._extract_key_indicators(asset, context_data)
                        print(f"Extracted overall sentiment: {extracted.get('overall_sentiment')}")
                        print(f"Key indicators:")
                        for key, indicator in extracted.get('key_indicators', {}).items():
                            print(
                                f"  {key}: sentiment={indicator.get('sentiment')}, confidence={indicator.get('confidence')}")

                    except json.JSONDecodeError as e:
                        print(f"❌ JSON decode error: {e}")
                        print(f"Raw response: {response.text[:500]}...")

                else:
                    print(f"❌ HTTP Error: {response.status_code}")
                    print(f"Response: {response.text[:200]}...")

            except Exception as e:
                print(f"❌ Exception for {asset}: {e}")
                import traceback
                traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(debug_context_extraction())
