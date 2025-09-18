import os
import json
import logging
import asyncio
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

# Import your updated modules
from config import Config
from context_fetcher import ContextFetcher
from llm_explainer import PortfolioExplainer

# NEW: Import the hybrid data system instead of historical_analyzer
from hybrid_data_fetcher import AutoDownloadingHybridDataManager as HybridDataManager
from flask_hybrid_integration import portfolio_api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
CORS(app)

# Initialize components with hybrid system
hybrid_data_manager = HybridDataManager()  # ← NEW: Hybrid system instead of historical_analyzer
context_fetcher = ContextFetcher()
llm_explainer = PortfolioExplainer()


@app.route('/')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Portfolio Explainer API",
        "version": "2.0.0 - Hybrid Live Data Edition",
        "timestamp": datetime.now().isoformat(),
        "data_strategy": "hybrid_historical_plus_live",
        "data_sources": ["csv_historical", "yfinance_live_today", "file_cache", "defaults"],
        "hybrid_features": {
            "historical_data": "CSV file up to X-1",
            "live_data": "YFinance for day X only",
            "concurrent_support": "Request deduplication enabled",
            "cache_management": "Smart expiration"
        }
    })


@app.route('/test-components')
def test_components():
    """Test all system components with hybrid data diagnostics"""
    results = {
        "hybrid_data_system": {"status": "unknown"},
        "azure_openai": {"status": "unknown"},
        "context_fetcher": {"status": "unknown"},
        "data_strategy": {"status": "unknown"}
    }

    # Test Hybrid Data System
    try:
        logger.info("Testing Hybrid Data System...")

        test_portfolios = {
            "current": {"Equities": 30, "Gold": 40, "Bitcoin": 20, "REITs": 10},
            "optimized": {"Equities": 35, "Gold": 35, "Bitcoin": 20, "REITs": 10}
        }

        # Test hybrid data fetch
        test_result = asyncio.run(
            portfolio_api.handle_portfolio_request({
                'portfolios': test_portfolios,
                'include_live_data': True
            })
        )

        if test_result and test_result.get('status') == 'success':
            data_sources = test_result.get('data_strategy', {})

            results["hybrid_data_system"] = {
                "status": "success",
                "assets_analyzed": len(test_result.get('portfolio_data', {})),
                "historical_sources": data_sources.get('historical_count', 0),
                "live_sources": data_sources.get('live_count', 0),
                "cache_hits": data_sources.get('cache_hits', 0),
                "data_strategy": "historical_X-1_live_X",
                "sample_asset_price": f"${test_result.get('portfolio_data', {}).get('Bitcoin', {}).get('current_price', 0):,.2f}"
            }
        else:
            results["hybrid_data_system"] = {
                "status": "failed",
                "error": "No hybrid data generated"
            }

    except Exception as e:
        results["hybrid_data_system"] = {"status": "error", "error": str(e)}

    # Test Azure OpenAI Configuration (same as before)
    try:
        if Config.AZURE_OPENAI_KEY and Config.AZURE_OPENAI_ENDPOINT:
            results["azure_openai"] = {
                "status": "configured",
                "deployment": Config.AZURE_OPENAI_DEPLOYMENT,
                "endpoint_configured": bool(Config.AZURE_OPENAI_ENDPOINT),
                "api_key_present": bool(Config.AZURE_OPENAI_KEY)
            }
        else:
            results["azure_openai"] = {
                "status": "not_configured",
                "error": "Missing Azure OpenAI configuration"
            }
    except Exception as e:
        results["azure_openai"] = {"status": "error", "error": str(e)}

    # Test Context Fetcher (same as before)
    try:
        if Config.ASSET_BACKEND_URL:
            results["context_fetcher"] = {
                "status": "configured",
                "backend_url": Config.ASSET_BACKEND_URL,
                "ready_for_requests": True
            }
        else:
            results["context_fetcher"] = {
                "status": "not_configured",
                "error": "Missing backend URL"
            }
    except Exception as e:
        results["context_fetcher"] = {"status": "error", "error": str(e)}

    # Test Data Strategy (NEW)
    try:
        config_status = hybrid_data_manager.get_system_status()

        results["data_strategy"] = {
            "status": "operational",
            "strategy": "hybrid_historical_plus_live",
            "historical_source": config_status.get('historical_source', 'unknown'),
            "live_source": config_status.get('live_source', 'unknown'),
            "cache_enabled": config_status.get('cache_enabled', False),
            "concurrent_support": config_status.get('concurrent_support', False),
            "performance": config_status.get('performance_tier', 'standard')
        }
    except Exception as e:
        results["data_strategy"] = {"status": "error", "error": str(e)}

    return jsonify(results)


@app.route('/data-status')
def data_status():
    """Get detailed hybrid data system status"""
    try:
        status = hybrid_data_manager.get_system_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/force-refresh', methods=['POST'])
def force_refresh():
    """Force refresh all cached data in hybrid system"""
    try:
        logger.info("Force refresh requested for hybrid system")
        results = asyncio.run(hybrid_data_manager.force_refresh_all())
        return jsonify({
            "status": "success",
            "message": "Hybrid cache refreshed",
            "assets_refreshed": len(results),
            "results": results
        })
    except Exception as e:
        logger.error(f"Force refresh failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/generate-portfolio-explanation', methods=['POST'])
def generate_portfolio_explanation():
    """Main endpoint for generating portfolio explanations with hybrid data"""
    try:
        # Validate request (same as before)
        if not request.is_json:
            return jsonify({
                "status": "error",
                "error": "Request must be JSON format",
                "error_code": "INVALID_REQUEST",
                "timestamp": datetime.now().isoformat()
            }), 400

        data = request.get_json()

        # Validate required fields (same as before)
        required_fields = ['job_id', 'current_portfolio', 'optimized_portfolio']
        missing_fields = [field for field in required_fields if field not in data]

        if missing_fields:
            return jsonify({
                "status": "error",
                "error": f"Missing required fields: {', '.join(missing_fields)}",
                "error_code": "INVALID_REQUEST",
                "timestamp": datetime.now().isoformat()
            }), 400

        # Extract parameters
        job_id = data['job_id']
        current_portfolio = data['current_portfolio']
        optimized_portfolio = data['optimized_portfolio']
        risk_profile = data.get('risk_profile', 'Balanced')

        # Validate risk profile (same as before)
        valid_risk_profiles = ['Aggressive', 'Balanced', 'Conservative']
        if risk_profile not in valid_risk_profiles:
            return jsonify({
                "status": "error",
                "error": f"Invalid risk_profile. Must be one of: {', '.join(valid_risk_profiles)}",
                "error_code": "INVALID_REQUEST",
                "timestamp": datetime.now().isoformat()
            }), 400

        logger.info(f"🔄 Processing portfolio explanation for job_id: {job_id}")
        logger.info(f"📊 Current: {current_portfolio}")
        logger.info(f"🎯 Optimized: {optimized_portfolio}")
        logger.info(f"⚖️ Risk Profile: {risk_profile}")

        # Process the request with hybrid system
        return asyncio.run(process_portfolio_explanation_hybrid(
            job_id, current_portfolio, optimized_portfolio, risk_profile
        ))

    except Exception as e:
        logger.error(f"Unexpected error in portfolio explanation: {e}")
        return jsonify({
            "status": "error",
            "error": f"Internal server error: {str(e)}",
            "error_code": "INTERNAL_ERROR",
            "timestamp": datetime.now().isoformat()
        }), 500


async def process_portfolio_explanation_hybrid(job_id, current_portfolio, optimized_portfolio, risk_profile):
    """Enhanced processing with hybrid historical + live data system"""
    try:
        # Step 1: Get hybrid data (historical + live)
        logger.info("Step 1: Getting hybrid data (historical up to X-1, live for day X)...")

        portfolios = {
            "current": current_portfolio,
            "optimized": optimized_portfolio
        }

        # Use the hybrid system through portfolio API
        portfolio_request = {
            'portfolios': portfolios,
            'include_live_data': True,
            'request_id': job_id
        }

        hybrid_result = await portfolio_api.handle_portfolio_request(portfolio_request)

        if not hybrid_result or hybrid_result.get('status') != 'success':
            logger.warning("Hybrid data fetch failed - using emergency fallback")
            historical_metrics = {}
            data_sources = {'fallback_count': 1}
        else:
            historical_metrics = hybrid_result.get('portfolio_data', {})
            data_sources = hybrid_result.get('data_strategy', {})

        # Log what we got from hybrid system
        logger.info(f"📈 Hybrid data summary:")
        logger.info(f"  Historical sources (X-1): {data_sources.get('historical_count', 0)}")
        logger.info(f"  Live sources (day X): {data_sources.get('live_count', 0)}")
        logger.info(f"  Cache hits: {data_sources.get('cache_hits', 0)}")

        for asset, data in historical_metrics.items():
            source = data.get('data_source', 'unknown')
            price = data.get('current_price', 0)
            logger.info(f"  {asset}: ${price:,.2f} (source: {source})")

        # Step 2: Fetch context data (same as before)
        logger.info(f"Step 2: Fetching context data for job_id: {job_id}")
        try:
            context_data = await context_fetcher.fetch_all_context_files(job_id)

            # Validate context data (same as before)
            if not context_data or all(not data.get('key_indicators') for data in context_data.values()):
                logger.warning(f"No valid context data found for job_id: {job_id}")
                return jsonify({
                    "status": "error",
                    "error": f"No valid context data found for job_id: {job_id}",
                    "error_code": "JOB_NOT_FOUND",
                    "job_id": job_id,
                    "timestamp": datetime.now().isoformat()
                }), 404

            logger.info(f"📋 Context fetched for: {list(context_data.keys())}")

        except Exception as e:
            logger.error(f"Context fetch failed: {e}")
            return jsonify({
                "status": "error",
                "error": f"Failed to fetch market context: {str(e)}",
                "error_code": "CONTEXT_FETCH_FAILED",
                "job_id": job_id,
                "timestamp": datetime.now().isoformat()
            }), 502

        # Step 3: Generate AI explanation (same as before)
        logger.info("Step 3: Generating AI explanation with hybrid data...")
        try:
            explanation = await llm_explainer.generate_portfolio_explanation(
                current_portfolio, optimized_portfolio, historical_metrics, context_data, risk_profile
            )

            if not explanation or 'error' in explanation:
                error_msg = explanation.get('error', 'Unknown LLM error') if explanation else 'No explanation generated'
                logger.error(f"LLM explanation failed: {error_msg}")
                return jsonify({
                    "status": "error",
                    "error": f"AI explanation generation failed: {error_msg}",
                    "error_code": "AZURE_OPENAI_ERROR",
                    "job_id": job_id,
                    "timestamp": datetime.now().isoformat()
                }), 503

            logger.info("🤖 AI explanation generated successfully")

        except Exception as e:
            logger.error(f"LLM explanation failed: {e}")
            return jsonify({
                "status": "error",
                "error": f"AI explanation generation failed: {str(e)}",
                "error_code": "AZURE_OPENAI_ERROR",
                "job_id": job_id,
                "timestamp": datetime.now().isoformat()
            }), 503

        # Step 4: Prepare enhanced response with hybrid system metadata
        logger.info("Step 4: Preparing response with hybrid system metadata...")

        response = {
            "status": "success",
            "job_id": job_id,
            "explanation": explanation,
            "metadata": {
                "assets_analyzed": len(historical_metrics),
                "context_files_fetched": len([data for data in context_data.values() if data.get('key_indicators')]),
                "risk_profile": risk_profile,
                "timestamp": datetime.now().isoformat(),
                "hybrid_performance": {
                    "historical_sources": data_sources.get('historical_count', 0),
                    "live_sources": data_sources.get('live_count', 0),
                    "cache_hits": data_sources.get('cache_hits', 0),
                    "total_requests": data_sources.get('total_requests', 0),
                    "concurrent_requests_handled": data_sources.get('concurrent_handled', 0)
                },
                "data_freshness": {
                    "historical_data": "Complete up to yesterday (X-1)",
                    "live_data": "Real-time for today (day X)",
                    "cache_efficiency": f"{data_sources.get('cache_hits', 0)}/{data_sources.get('total_requests', 1)}"
                }
            }
        }

        logger.info(f"Portfolio explanation completed successfully!")
        logger.info(f"Hybrid performance: {response['metadata']['hybrid_performance']}")
        logger.info(f"Cache efficiency: {response['metadata']['data_freshness']['cache_efficiency']}")

        return jsonify(response)

    except Exception as e:
        logger.error(f"Unexpected error in hybrid processing: {e}")
        return jsonify({
            "status": "error",
            "error": f"Processing failed: {str(e)}",
            "error_code": "INTERNAL_ERROR",
            "job_id": job_id,
            "timestamp": datetime.now().isoformat()
        }), 500


# NEW: Additional endpoints for hybrid system
@app.route('/api/portfolio/explain', methods=['POST'])
def get_portfolio_explanation():
    """Alternative endpoint that directly uses hybrid system (compatible with existing clients)"""
    try:
        request_data = request.get_json()

        # Use hybrid data fetching
        portfolio_data = asyncio.run(
            portfolio_api.handle_portfolio_request(request_data)
        )

        # Continue with existing logic...
        if portfolio_data and portfolio_data.get('status') == 'success':
            return jsonify({
                "status": "success",
                "portfolio_data": portfolio_data['portfolio_data'],
                "data_strategy": "hybrid_live_historical",
                "hybrid_performance": portfolio_data.get('data_strategy', {}),
                "timestamp": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "status": "error",
                "error": "Failed to fetch portfolio data",
                "timestamp": datetime.now().isoformat()
            }), 500

    except Exception as e:
        logger.error(f"Portfolio explanation error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/data/status')
def get_data_status():
    """Get hybrid system status"""
    try:
        status = hybrid_data_manager.get_system_status()
        return jsonify({
            "system_status": status,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/data/refresh', methods=['POST'])
def refresh_data():
    """Force refresh hybrid system data"""
    try:
        results = asyncio.run(hybrid_data_manager.force_refresh_all())
        return jsonify({
            "status": "success",
            "refresh_results": results,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/assets/<asset>/live')
def get_live_asset_data(asset):
    """Get live data for specific asset"""
    try:
        live_data = asyncio.run(hybrid_data_manager.get_live_data_for_asset(asset))
        return jsonify({
            "asset": asset,
            "live_data": live_data,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Error handlers (same as before)
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "error": "Endpoint not found",
        "error_code": "NOT_FOUND",
        "timestamp": datetime.now().isoformat()
    }), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "error": "Internal server error",
        "error_code": "INTERNAL_ERROR",
        "timestamp": datetime.now().isoformat()
    }), 500


if __name__ == '__main__':
    # Get port from environment variable (Railway sets this)
    port = int(os.environ.get('PORT', 5001))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

    logger.info(f"Starting Portfolio Explainer API with Hybrid Data System")
    logger.info(f"Port: {port}")
    logger.info(f"Debug: {debug}")
    logger.info(f"Features: Concurrent request handling, Smart caching, Fallback layers")


    app.run(host='0.0.0.0', port=port, debug=debug)