import httpx
import asyncio
import logging
from config import Config

logger = logging.getLogger(__name__)


class ContextFetcher:
    def __init__(self, base_url=None):
        self.base_url = base_url or Config.ASSET_BACKEND_URL

    async def fetch_all_context_files(self, job_id):
        """Fetch all 4 context files and extract key indicators"""
        logger.info(f"Fetching context files for job_id: {job_id}")

        context_files = {}
        assets = ['NIFTY50', 'GOLD', 'BITCOIN', 'REIT']

        async with httpx.AsyncClient(timeout=30.0) as client:
            for asset in assets:
                try:
                    url = f"{self.base_url}/jobs/{job_id}/context/{asset}"
                    logger.info(f"Fetching {asset} context from: {url}")

                    response = await client.get(url)
                    if response.status_code == 200:
                        response_data = response.json()

                        # Handle response format - check if there's a context_data wrapper
                        if 'context_data' in response_data:
                            raw_context = response_data['context_data']
                            logger.info(f"Using context_data wrapper for {asset}")
                        else:
                            raw_context = response_data
                            logger.info(f"Using direct response for {asset}")

                        context_files[asset] = self._extract_key_indicators(asset, raw_context)
                        logger.info(f"Successfully fetched {asset} context")
                    else:
                        logger.warning(f"Failed to fetch {asset} context: {response.status_code}")
                        context_files[asset] = self._get_default_context(asset)

                except Exception as e:
                    logger.error(f"Error fetching {asset} context: {e}")
                    context_files[asset] = self._get_default_context(asset)

        return context_files

    def _extract_key_indicators(self, asset, raw_context):
        """Extract and structure key indicators from context file"""
        try:
            # Extract main data
            sentiment_analysis = raw_context.get('sentiment_analysis', {})
            overall_sentiment = sentiment_analysis.get('overall_sentiment', 0)
            confidence_level = sentiment_analysis.get('confidence_level', 0.5)

            # Extract component breakdown
            component_breakdown = raw_context.get('component_breakdown', {})
            logger.info(f"{asset} component_breakdown keys: {list(component_breakdown.keys())}")

            if asset == 'NIFTY50':
                return {
                    "overall_sentiment": overall_sentiment,
                    "confidence_level": confidence_level,
                    "key_indicators": {
                        "fii_dii_flows": self._get_component_insight(component_breakdown, 'fii_dii_flows'),
                        "technical_analysis": self._get_component_insight(component_breakdown, 'technical_analysis'),
                        "market_sentiment": self._get_component_insight(component_breakdown, 'market_sentiment_vix'),
                        "rbi_policy": self._get_component_insight(component_breakdown, 'rbi_interest_rates'),
                        "global_factors": self._get_component_insight(component_breakdown, 'global_factors')
                    }
                }

            elif asset == 'GOLD':
                return {
                    "overall_sentiment": overall_sentiment,
                    "confidence_level": confidence_level,
                    "key_indicators": {
                        "price_momentum": self._get_component_insight(component_breakdown, 'technical_analysis'),
                        "usd_inr_impact": self._get_component_insight(component_breakdown, 'real_interest_rates'),
                        "interest_rate_impact": self._get_component_insight(component_breakdown, 'real_interest_rates'),
                        "inflation_indicators": self._get_component_insight(component_breakdown, 'currency_debasement'),
                        "global_sentiment": self._get_component_insight(component_breakdown, 'central_bank_sentiment')
                    }
                }

            elif asset == 'BITCOIN':
                return {
                    "overall_sentiment": overall_sentiment,
                    "confidence_level": confidence_level,
                    "key_indicators": {
                        "micro_momentum": self._get_component_insight(component_breakdown, 'micro_momentum'),
                        "funding_rates": self._get_component_insight(component_breakdown, 'funding_basis'),
                        "liquidity_analysis": self._get_component_insight(component_breakdown, 'liquidity'),
                        "order_flow": self._get_component_insight(component_breakdown, 'orderflow')
                    }
                }

            elif asset == 'REIT':
                return {
                    "overall_sentiment": overall_sentiment,
                    "confidence_level": confidence_level,
                    "key_indicators": {
                        "technical_momentum": self._get_component_insight(component_breakdown, 'technical_momentum'),
                        "yield_spread": self._get_component_insight(component_breakdown, 'yield_spread'),
                        "accumulation_flow": self._get_component_insight(component_breakdown, 'accumulation_flow'),
                        "liquidity_risk": self._get_component_insight(component_breakdown, 'liquidity_risk')
                    }
                }

        except Exception as e:
            logger.error(f"Error extracting indicators for {asset}: {e}")
            return self._get_default_context(asset)

    def _get_component_insight(self, breakdown, component_name):
        """Extract specific component insight"""
        component = breakdown.get(component_name, {})

        if not component:
            logger.warning(f"Component '{component_name}' not found in breakdown. Available: {list(breakdown.keys())}")
            return {
                "sentiment": 0.0,
                "confidence": 0.5,
                "description": f'No {component_name} data available'
            }

        # Extract values with proper type conversion
        sentiment = float(component.get('sentiment', 0.0))
        confidence = float(component.get('confidence', 0.5))
        description = component.get('description', f'{component_name} analysis')

        logger.info(f"{component_name}: sentiment={sentiment:.3f}, confidence={confidence:.3f}")

        return {
            "sentiment": sentiment,
            "confidence": confidence,
            "description": description
        }

    def _get_default_context(self, asset):
        """Return default context when fetch fails"""
        return {
            "overall_sentiment": 0.0,
            "confidence_level": 0.3,
            "key_indicators": {
                "error": f"Failed to fetch {asset} context data"
            }
        }
