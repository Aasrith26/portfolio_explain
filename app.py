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
from historical_analyzer import EnhancedHistoricalAnalyzer  # ← Updated import
from llm_explainer import PortfolioExplainer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
CORS(app)

# Initialize components with enhanced versions
historical_analyzer = EnhancedHistoricalAnalyzer()  # ← Uses cache-first approach
context_fetcher = ContextFetcher()
llm_explainer = PortfolioExplainer()


@app.route('/')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Portfolio Explainer API",
        "version": "1.0.0 - Live Data Edition",
        "timestamp": datetime.now().isoformat(),
        "cache_system": "file_based",
        "data_sources": ["live_yfinance", "file_cache", "csv_fallback", "defaults"]
    })


@app.route('/test-components')
def test_components():
    """Test all system components with detailed diagnostics"""
    results = {
        "historical_analyzer": {"status": "unknown"},
        "azure_openai": {"status": "unknown"},
        "context_fetcher": {"status": "unknown"},
        "cache_system": {"status": "unknown"}
    }

    # Test Historical Analyzer (Cache-First System)
    try:
        logger.info("Testing Enhanced Historical Analyzer...")

        test_portfolios = {
            "current": {"Equities": 30, "Gold": 40, "Bitcoin": 20, "REITs": 10},
            "optimized": {"Equities": 35, "Gold": 35, "Bitcoin": 20, "REITs": 10}
        }

        # Get data status first
        data_status = historical_analyzer.get_data_status()

        # Test calculation
        historical_data = historical_analyzer.calculate_all_metrics(test_portfolios)

        if historical_data and len(historical_data) > 0:
            # Count data sources
            cache_hits = sum(1 for data in historical_data.values()
                             if data.get('data_source') in ['live_pipeline', 'direct_yfinance'])

            results["historical_analyzer"] = {
                "status": "success",
                "assets_analyzed": len(historical_data),
                "cache_available": data_status['cache_available'],
                "cache_type": data_status['cache_type'],
                "fresh_data_fetched": cache_hits,
                "sample_asset_price": f"${historical_data.get('Bitcoin', {}).get('current_stats', {}).get('current_price', 0):,.2f}"
            }
        else:
            results["historical_analyzer"] = {
                "status": "failed",
                "error": "No historical data generated"
            }

    except Exception as e:
        results["historical_analyzer"] = {"status": "error", "error": str(e)}

    # Test Azure OpenAI Configuration
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

    # Test Context Fetcher
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

    # Test Cache System
    try:
        cache_status = historical_analyzer.get_data_status()
        asset_cache_status = cache_status.get('assets', {})

        cached_assets = sum(1 for status in asset_cache_status.values()
                            if status.get('cache_file_exists', False))
        fresh_assets = sum(1 for status in asset_cache_status.values()
                           if status.get('is_fresh', False))

        results["cache_system"] = {
            "status": "operational",
            "cache_directory": cache_status.get('cache_directory'),
            "total_cached_assets": cached_assets,
            "fresh_cached_assets": fresh_assets,
            "cache_performance": f"{fresh_assets}/{len(asset_cache_status)} assets fresh"
        }

    except Exception as e:
        results["cache_system"] = {"status": "error", "error": str(e)}

    return jsonify(results)


@app.route('/cache-status')
def cache_status():
    """Get detailed cache status"""
    try:
        status = historical_analyzer.get_data_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/force-refresh', methods=['POST'])
def force_refresh():
    """Force refresh all cached data"""
    try:
        logger.info("Force refresh requested")
        results = historical_analyzer.force_refresh_all()
        return jsonify({
            "status": "success",
            "message": "Cache refreshed",
            "assets_refreshed": len(results),
            "results": results
        })
    except Exception as e:
        logger.error(f"Force refresh failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/generate-portfolio-explanation', methods=['POST'])
def generate_portfolio_explanation():
    """Main endpoint for generating portfolio explanations"""
    try:
        # Validate request
        if not request.is_json:
            return jsonify({
                "status": "error",
                "error": "Request must be JSON format",
                "error_code": "INVALID_REQUEST",
                "timestamp": datetime.now().isoformat()
            }), 400

        data = request.get_json()

        # Validate required fields
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

        # Validate risk profile
        valid_risk_profiles = ['Aggressive', 'Balanced', 'Conservative']
        if risk_profile not in valid_risk_profiles:
            return jsonify({
                "status": "error",
                "error": f"Invalid risk_profile. Must be one of: {', '.join(valid_risk_profiles)}",
                "error_code": "INVALID_REQUEST",
                "timestamp": datetime.now().isoformat()
            }), 400

        logger.info(f"   Processing portfolio explanation for job_id: {job_id}")
        logger.info(f"   Current: {current_portfolio}")
        logger.info(f"   Optimized: {optimized_portfolio}")
        logger.info(f"   Risk Profile: {risk_profile}")

        # Process the request
        return asyncio.run(process_portfolio_explanation_enhanced(
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


async def process_portfolio_explanation_enhanced(job_id, current_portfolio, optimized_portfolio, risk_profile):
    """Enhanced processing with cache-first approach"""
    try:
        # Step 1: Calculate historical metrics (CACHE-FIRST)
        logger.info("Step 1: Calculating historical metrics (cache-first)...")
        portfolios = {
            "current": current_portfolio,
            "optimized": optimized_portfolio
        }

        # This will use cache-first approach automatically
        historical_metrics = historical_analyzer.calculate_all_metrics(portfolios)

        if not historical_metrics:
            logger.warning("No historical metrics calculated - using emergency fallback")
            historical_metrics = {}

        # Log what we got
        for asset, data in historical_metrics.items():
            source = data.get('data_source', 'unknown')
            price = data.get('current_stats', {}).get('current_price', 0)
            logger.info(f"   {asset}: ${price:,.2f} (source: {source})")

        # Step 2: Fetch context data
        logger.info(f"Step 2: Fetching context data for job_id: {job_id}")
        try:
            context_data = await context_fetcher.fetch_all_context_files(job_id)

            # Validate context data
            if not context_data or all(not data.get('key_indicators') for data in context_data.values()):
                logger.warning(f"No valid context data found for job_id: {job_id}")
                return jsonify({
                    "status": "error",
                    "error": f"No valid context data found for job_id: {job_id}",
                    "error_code": "JOB_NOT_FOUND",
                    "job_id": job_id,
                    "timestamp": datetime.now().isoformat()
                }), 404

            logger.info(f"   Context fetched for: {list(context_data.keys())}")

        except Exception as e:
            logger.error(f"Context fetch failed: {e}")
            return jsonify({
                "status": "error",
                "error": f"Failed to fetch market context: {str(e)}",
                "error_code": "CONTEXT_FETCH_FAILED",
                "job_id": job_id,
                "timestamp": datetime.now().isoformat()
            }), 502

        # Step 3: Generate AI explanation with enhanced data
        logger.info("Step 3: Generating AI explanation with live data...")
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

            logger.info("AI explanation generated successfully")

        except Exception as e:
            logger.error(f"LLM explanation failed: {e}")
            return jsonify({
                "status": "error",
                "error": f"AI explanation generation failed: {str(e)}",
                "error_code": "AZURE_OPENAI_ERROR",
                "job_id": job_id,
                "timestamp": datetime.now().isoformat()
            }), 503

        # Step 4: Prepare enhanced response with data source tracking
        logger.info("Step 4: Preparing response with metadata...")

        # Count data sources for metadata
        historical_sources = {}
        for asset, data in historical_metrics.items():
            source = data.get('data_source', 'unknown')
            historical_sources[source] = historical_sources.get(source, 0) + 1

        response = {
            "status": "success",
            "job_id": job_id,
            "explanation": explanation,
            "metadata": {
                "historical_assets_analyzed": len(historical_metrics),
                "context_files_fetched": len([data for data in context_data.values() if data.get('key_indicators')]),
                "risk_profile": risk_profile,
                "timestamp": datetime.now().isoformat(),
                "data_sources": historical_sources,
                "cache_performance": {
                    "cache_hits": historical_sources.get('live_pipeline', 0) + historical_sources.get('direct_yfinance',
                                                                                                      0),
                    "fallbacks": historical_sources.get('csv_fallback', 0) + historical_sources.get('default_fallback',
                                                                                                    0)
                }
            }
        }

        logger.info(f"   Portfolio explanation completed successfully!")
        logger.info(f"   Cache performance: {response['metadata']['cache_performance']}")

        return jsonify(response)

    except Exception as e:
        logger.error(f"Unexpected error in enhanced processing: {e}")
        return jsonify({
            "status": "error",
            "error": f"Processing failed: {str(e)}",
            "error_code": "INTERNAL_ERROR",
            "job_id": job_id,
            "timestamp": datetime.now().isoformat()
        }), 500


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
    # Get port from environment variable
    port = int(os.environ.get('PORT', Config.PORT))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

    logger.info(f" Starting Portfolio Explainer API with Enhanced Live Data System")
    logger.info(f"   Port: {port}")
    logger.info(f"   Debug: {debug}")
    logger.info(f"   Cache System: File-based")
    logger.info(f"   Data Priority: Cache → Live → CSV → Defaults")

    app.run(host='0.0.0.0', port=port, debug=debug)
