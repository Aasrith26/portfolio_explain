from flask import Flask, request, jsonify
from flask_cors import CORS
import asyncio
import logging
from datetime import datetime
import traceback

from config import Config
from historical_analyzer import HistoricalAnalyzer
from context_fetcher import ContextFetcher
from llm_explainer import PortfolioExplainer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/app.log')
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Initialize components
historical_analyzer = HistoricalAnalyzer()
context_fetcher = ContextFetcher()
explainer = PortfolioExplainer()


@app.route('/')
def health_check():
    """Basic health check"""
    return jsonify({
        "status": "healthy",
        "service": "Portfolio Explainer API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    })


@app.route('/test-components')
def test_components():
    """Test all components"""
    results = {}

    # Test historical analyzer
    try:
        historical_metrics = historical_analyzer.calculate_returns_and_metrics()
        results["historical_analyzer"] = {
            "status": "success",
            "assets_analyzed": len(historical_metrics)
        }
    except Exception as e:
        results["historical_analyzer"] = {
            "status": "error",
            "error": str(e)
        }

    # Test Azure OpenAI connection
    try:
        # Simple test
        test_response = explainer.client.chat.completions.create(
            messages=[{"role": "user", "content": "Hello"}],
            max_completion_tokens=10,
            model=explainer.deployment
        )
        results["azure_openai"] = {
            "status": "success",
            "model": explainer.deployment
        }
    except Exception as e:
        results["azure_openai"] = {
            "status": "error",
            "error": str(e)
        }

    return jsonify(results)


@app.route('/generate-portfolio-explanation', methods=['POST'])
def generate_portfolio_explanation():
    """
    Generate comprehensive portfolio explanation

    Expected Input:
    {
        "job_id": "uuid-from-sentiment-analysis",
        "current_portfolio": {"Gold": 40.0, "Equities": 30.0, "REITs": 10.0, "Bitcoin": 20.0},
        "optimized_portfolio": {"Gold": 42.5, "Equities": 32.8, "REITs": 14.7, "Bitcoin": 10.0},
        "risk_profile": "Sharpe"
    }
    """
    try:
        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        # Extract required fields
        job_id = data.get('job_id')
        current_portfolio = data.get('current_portfolio')
        optimized_portfolio = data.get('optimized_portfolio')
        risk_profile = data.get('risk_profile', 'Balanced')

        # Validate inputs
        if not job_id:
            return jsonify({"error": "job_id is required"}), 400
        if not current_portfolio or not optimized_portfolio:
            return jsonify({"error": "Both current_portfolio and optimized_portfolio are required"}), 400

        logger.info(f"Processing explanation request for job_id: {job_id}")
        logger.info(f"Risk profile: {risk_profile}")

        # Step 1: Calculate historical metrics
        logger.info("Step 1: Calculating historical metrics")
        historical_metrics = historical_analyzer.calculate_returns_and_metrics()

        # Step 2: Fetch context files
        logger.info("Step 2: Fetching context files")
        context_data = asyncio.run(context_fetcher.fetch_all_context_files(job_id))

        # Step 3: Generate LLM explanation
        logger.info("Step 3: Generating LLM explanation")
        explanation = asyncio.run(explainer.generate_portfolio_explanation(
            current_portfolio, optimized_portfolio,
            historical_metrics, context_data, risk_profile
        ))

        # Check for errors in explanation
        if "error" in explanation:
            logger.error(f"LLM explanation error: {explanation['error']}")
            return jsonify({
                "status": "error",
                "error": explanation["error"],
                "job_id": job_id
            }), 500

        # Success response
        response = {
            "status": "success",
            "job_id": job_id,
            "explanation": explanation,
            "metadata": {
                "historical_assets_analyzed": len(historical_metrics),
                "context_files_fetched": len(context_data),
                "risk_profile": risk_profile,
                "timestamp": datetime.now().isoformat()
            }
        }

        logger.info(f"Successfully generated explanation for job_id: {job_id}")
        return jsonify(response)

    except Exception as e:
        logger.error(f"Portfolio explanation failed: {e}")
        logger.error(traceback.format_exc())

        return jsonify({
            "status": "error",
            "error": str(e),
            "job_id": data.get('job_id', 'unknown') if data else 'unknown'
        }), 500


@app.route('/test-explanation', methods=['POST'])
def test_explanation():
    """Test endpoint with sample data"""
    sample_data = {
        "job_id": "test-job-id",
        "current_portfolio": {
            "Gold": 40.0,
            "Equities": 30.0,
            "REITs": 10.0,
            "Bitcoin": 20.0
        },
        "optimized_portfolio": {
            "Gold": 42.5,
            "Equities": 32.8,
            "REITs": 14.7,
            "Bitcoin": 10.0
        },
        "risk_profile": "Balanced"
    }

    # Use the main function with sample data
    return generate_portfolio_explanation()


if __name__ == '__main__':
    logger.info("Starting Portfolio Explainer API")
    logger.info(f"Configuration: {Config.FLASK_ENV} mode")

    app.run(
        host='0.0.0.0',
        port=Config.PORT,
        debug=Config.FLASK_DEBUG
    )
