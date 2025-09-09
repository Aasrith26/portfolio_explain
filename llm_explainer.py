import os
import json
import logging
from openai import AzureOpenAI
from config import Config

logger = logging.getLogger(__name__)


class PortfolioExplainer:
    def __init__(self):
        self.client = AzureOpenAI(
            api_version=Config.AZURE_OPENAI_VERSION,
            azure_endpoint=Config.AZURE_OPENAI_ENDPOINT,
            api_key=Config.AZURE_OPENAI_KEY,
        )
        self.deployment = Config.AZURE_OPENAI_DEPLOYMENT

    async def generate_portfolio_explanation(self, current_portfolio, optimized_portfolio,
                                             historical_metrics, context_data, risk_profile):
        """Generate professional portfolio explanation with key market data"""

        logger.info("Generating professional portfolio explanation with market data")

        # Prepare professional prompt with market data
        prompt = self._create_professional_advisory_prompt(
            current_portfolio, optimized_portfolio,
            context_data, risk_profile
        )

        try:
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a senior portfolio manager at a prestigious investment firm. Provide professional investment recommendations using relevant market data, economic indicators, and key metrics to support your decisions. Include specific numbers when they strengthen your investment rationale. Speak with authority and expertise, but avoid overwhelming technical jargon or confidence scores."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_completion_tokens=8000,
                model=self.deployment,
                response_format={"type": "json_object"}
            )

            # Parse JSON response
            explanation_text = response.choices[0].message.content
            logger.info(f"LLM response received: {len(explanation_text)} characters")

            explanation_json = json.loads(explanation_text)

            # Add historical metrics to response after LLM processing
            explanation_json = self._add_historical_metrics_to_response(
                explanation_json, historical_metrics
            )

            return explanation_json

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {e}")
            return {"error": f"Invalid JSON response from LLM: {e}"}
        except Exception as e:
            logger.error(f"LLM explanation failed: {e}")
            return {"error": f"LLM explanation failed: {e}"}

    def _create_professional_advisory_prompt(self, current_portfolio, optimized_portfolio,
                                             context_data, risk_profile):
        """Create professional prompt with key market data"""

        # Calculate portfolio changes with specific percentages
        changes_data = self._create_detailed_changes_analysis(current_portfolio, optimized_portfolio)

        # Extract key market data and metrics
        market_data = self._extract_key_market_data(context_data)

        return f"""
You are providing investment advice to a client based on comprehensive market analysis. Use specific market data and key metrics to justify your recommendations.

CLIENT PORTFOLIO REBALANCING:
Current Allocation: {json.dumps(current_portfolio)}%
Recommended Allocation: {json.dumps(optimized_portfolio)}%
Risk Profile: {risk_profile}
Key Changes: {changes_data}

CURRENT MARKET DATA & ANALYSIS:
{market_data}

ADVISORY GUIDELINES:
- Use specific market metrics, levels, and data points to support recommendations
- Reference key economic indicators, market levels, and important financial metrics
- Explain the reasoning behind each allocation using concrete market data
- Include relevant percentages, market levels, and key performance indicators
- Avoid confidence scores, sentiment numbers
- Make explanations sound authoritative and data-driven
- Each asset explanation should be 5-6 sentences with supporting market data

PROFESSIONAL ADVISORY OUTPUT (Strict JSON):
{{
  "portfolio_analysis": {{
    "overall_explanation": "5-6 sentences explaining the overall investment strategy using market data, economic indicators, and key metrics that support the {risk_profile} allocation approach",
    "allocation_rationale": "4-5 sentences explaining the specific allocation percentages using current market conditions, valuations, and key performance metrics",

    "assets": {{
      "Equities": {{
        "allocation_pct": {optimized_portfolio.get('Equities', 0)},
        "change_from_current": "{self._get_precise_change('Equities', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining the equity allocation using specific market data such as FII/DII flow numbers, index levels, VIX readings, repo rates, and global market performances. Reference actual market metrics and economic indicators.",
        "key_market_data": {{
          "institutional_flows": "Specific FII/DII flow data and institutional activity metrics",
          "market_levels": "Current index levels, support/resistance, and technical indicators", 
          "policy_metrics": "RBI repo rate, policy stance, and monetary policy impact",
          "sentiment_indicators": "VIX levels, fear/greed index, and market sentiment metrics",
          "global_context": "US market performance, DXY levels, and international influences"
        }}
      }},
      "Gold": {{
        "allocation_pct": {optimized_portfolio.get('Gold', 0)},
        "change_from_current": "{self._get_precise_change('Gold', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining gold allocation using specific data like gold prices, USD/INR levels, real interest rates, inflation metrics, and central bank gold purchases. Include relevant economic data and market metrics.",
        "key_market_data": {{
          "price_levels": "Current gold prices, key support/resistance levels, and price momentum data",
          "currency_data": "USD/INR exchange rates, currency volatility, and impact analysis",
          "interest_rates": "Real interest rate levels, yield curves, and rate environment impact", 
          "inflation_metrics": "CPI data, inflation expectations, and debasement indicators",
          "cb_activity": "Central bank gold purchases, reserve changes, and policy impacts"
        }}
      }},
      "Bitcoin": {{
        "allocation_pct": {optimized_portfolio.get('Bitcoin', 0)},
        "change_from_current": "{self._get_precise_change('Bitcoin', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining bitcoin allocation using specific metrics like price levels, funding rates, RSI readings, trading volumes, and institutional adoption data. Reference concrete crypto market indicators.",
        "key_market_data": {{
          "price_momentum": "Current bitcoin price levels, RSI readings, and momentum indicators",
          "funding_metrics": "Perpetual funding rates, basis spreads, and derivative market data",
          "liquidity_data": "Trading volumes, order book depth, and market liquidity metrics", 
          "flow_analysis": "Institutional flows, exchange inflows/outflows, and adoption metrics"
        }}
      }},
      "REITs": {{
        "allocation_pct": {optimized_portfolio.get('REITs', 0)},
        "change_from_current": "{self._get_precise_change('REITs', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining REIT allocation using specific data like dividend yields, price-to-book ratios, occupancy rates, interest rate spreads, and real estate market metrics. Include relevant property market indicators.",
        "key_market_data": {{
          "valuation_metrics": "Current REIT P/B ratios, dividend yields, and valuation indicators",
          "yield_analysis": "REIT yields vs 10-year G-Sec spreads and income comparison metrics",
          "market_activity": "REIT trading volumes, institutional ownership, and market participation", 
          "property_fundamentals": "Occupancy rates, rental growth, and underlying real estate metrics"
        }}
      }}
    }},

    "portfolio_level": {{
      "risk_profile": "{risk_profile}",
      "market_environment": "Current market conditions using specific economic data, policy rates, inflation numbers, and key market indicators that influence portfolio positioning",
      "diversification_metrics": "How the allocation spreads risk using correlation data, volatility measures, and diversification benefits with specific percentages",
      "timing_analysis": "Why current market levels, valuations, and economic indicators make this an opportune time for rebalancing",
      "performance_outlook": "Expected portfolio characteristics using historical data, current valuations, and market cycle positioning"
    }}
  }}
}}

Use specific numbers, market levels, rates, and key metrics throughout to support professional investment decisions.
"""

    def _create_detailed_changes_analysis(self, current_portfolio, optimized_portfolio):
        """Create detailed analysis of portfolio changes with specific data"""
        changes = []
        for asset in optimized_portfolio:
            current_pct = current_portfolio.get(asset, 0)
            optimized_pct = optimized_portfolio[asset]
            change = optimized_pct - current_pct

            if change != 0:
                changes.append(f"{asset}: {current_pct}% → {optimized_pct}% ({change:+.1f}%)")
            else:
                changes.append(f"{asset}: maintained at {optimized_pct}%")

        return "; ".join(changes)

    def _get_precise_change(self, asset, current_portfolio, optimized_portfolio):
        """Get precise change description with numbers"""
        current_pct = current_portfolio.get(asset, 0)
        optimized_pct = optimized_portfolio[asset]
        change = optimized_pct - current_pct

        if change > 0:
            return f"increased by {change:.1f}% (from {current_pct}% to {optimized_pct}%)"
        elif change < 0:
            return f"reduced by {abs(change):.1f}% (from {current_pct}% to {optimized_pct}%)"
        else:
            return f"maintained at {optimized_pct}%"

    def _extract_key_market_data(self, context_data):
        """Extract key market data and metrics for professional analysis"""
        market_sections = []

        for asset, data in context_data.items():
            overall_sentiment = data.get('overall_sentiment', 0)
            indicators = data.get('key_indicators', {})

            if asset == 'NIFTY50':
                sentiment_direction = self._sentiment_to_market_direction(overall_sentiment)
                market_sections.append(f"""
INDIAN EQUITY MARKET DATA:
Market Direction: {sentiment_direction} based on comprehensive analysis
Institutional Activity: {self._extract_market_insight(indicators.get('fii_dii_flows', {}))}
Technical Analysis: {self._extract_market_insight(indicators.get('technical_analysis', {}))}
Market Sentiment: {self._extract_market_insight(indicators.get('market_sentiment', {}))}
RBI Policy Impact: {self._extract_market_insight(indicators.get('rbi_policy', {}))}
Global Factors: {self._extract_market_insight(indicators.get('global_factors', {}))}
                """)

            elif asset == 'GOLD':
                sentiment_direction = self._sentiment_to_market_direction(overall_sentiment)
                market_sections.append(f"""
GOLD MARKET DATA:
Price Direction: {sentiment_direction} based on multiple factors
Technical Momentum: {self._extract_market_insight(indicators.get('price_momentum', {}))}
Currency Impact: {self._extract_market_insight(indicators.get('usd_inr_impact', {}))}
Interest Rate Environment: {self._extract_market_insight(indicators.get('interest_rate_impact', {}))}
Inflation Indicators: {self._extract_market_insight(indicators.get('inflation_indicators', {}))}
Central Bank Activity: {self._extract_market_insight(indicators.get('global_sentiment', {}))}
                """)

            elif asset == 'BITCOIN':
                sentiment_direction = self._sentiment_to_market_direction(overall_sentiment)
                market_sections.append(f"""
BITCOIN MARKET DATA:
Market Momentum: {sentiment_direction} across key metrics
Price Momentum: {self._extract_market_insight(indicators.get('micro_momentum', {}))}
Funding Conditions: {self._extract_market_insight(indicators.get('funding_rates', {}))}
Liquidity Analysis: {self._extract_market_insight(indicators.get('liquidity_analysis', {}))}
Order Flow: {self._extract_market_insight(indicators.get('order_flow', {}))}
                """)

            elif asset == 'REIT':
                sentiment_direction = self._sentiment_to_market_direction(overall_sentiment)
                market_sections.append(f"""
REIT MARKET DATA:
Sector Outlook: {sentiment_direction} based on fundamental analysis
Technical Momentum: {self._extract_market_insight(indicators.get('technical_momentum', {}))}
Yield Environment: {self._extract_market_insight(indicators.get('yield_spread', {}))}
Investment Flows: {self._extract_market_insight(indicators.get('accumulation_flow', {}))}
Liquidity Conditions: {self._extract_market_insight(indicators.get('liquidity_risk', {}))}
                """)

        return "\n".join(market_sections)

    def _extract_market_insight(self, indicator_data):
        """Extract market insight preserving important data points"""
        description = indicator_data.get('description', 'Market conditions remain stable')
        sentiment = indicator_data.get('sentiment', 0)

        # Add market direction context if significant
        if abs(sentiment) > 0.2:
            if sentiment > 0.2:
                direction_context = " (showing positive momentum)"
            else:
                direction_context = " (showing defensive characteristics)"
            description += direction_context

        return description

    def _sentiment_to_market_direction(self, sentiment):
        """Convert sentiment to market direction with some numerical context"""
        if sentiment > 0.3:
            return "Strong bullish trend"
        elif sentiment > 0.1:
            return "Constructive upward bias"
        elif sentiment > -0.1:
            return "Neutral with mixed signals"
        elif sentiment > -0.3:
            return "Cautious downward bias"
        else:
            return "Defensive positioning warranted"

    def _add_historical_metrics_to_response(self, explanation_json, historical_metrics):
        """Add historical metrics to the LLM response after processing"""

        try:
            # Add historical returns and risk metrics to each asset
            assets = explanation_json.get('portfolio_analysis', {}).get('assets', {})

            for asset_name, asset_data in assets.items():
                if asset_name in historical_metrics:
                    # Add historical returns
                    asset_data['historical_returns'] = historical_metrics[asset_name].get('historical_returns', {})

                    # Add risk metrics
                    asset_data['risk_metrics'] = historical_metrics[asset_name].get('risk_metrics', {})

                    # Add current stats
                    asset_data['current_stats'] = historical_metrics[asset_name].get('current_stats', {})

            return explanation_json

        except Exception as e:
            logger.error(f"Error adding historical metrics: {e}")
            return explanation_json


# Test function
def test_professional_explainer():
    explainer = PortfolioExplainer()

    # Sample data for testing
    sample_current = {"Equities": 30.0, "Gold": 40.0, "Bitcoin": 20.0, "REITs": 10.0}
    sample_optimized = {"Equities": 32.8, "Gold": 42.5, "Bitcoin": 10.0, "REITs": 14.7}

    # Sample context data with some numerical elements
    sample_context = {
        "NIFTY50": {
            "overall_sentiment": 0.25,
            "key_indicators": {
                "fii_dii_flows": {"sentiment": 0.35,
                                  "description": "FII net buying of ₹2,500 crores indicates strong institutional support"},
                "technical_analysis": {"sentiment": 0.28,
                                       "description": "NIFTY50 at 24,850 shows bullish breakout above 24,500 resistance"}
            }
        },
        "Gold": {
            "overall_sentiment": 0.40,
            "key_indicators": {
                "price_momentum": {"sentiment": 1.0,
                                   "description": "Gold trading at $2,650/oz with strong technical momentum"},
                "usd_inr_impact": {"sentiment": 0.45, "description": "USD/INR at 83.2 supports gold in rupee terms"}
            }
        }
    }

    sample_historical = {
        "Equities": {
            "historical_returns": {"1_year": "12.5%", "5_years_avg": "9.8%"},
            "risk_metrics": {"volatility": "16.2%", "max_drawdown": "-22%"}
        }
    }

    try:
        import asyncio
        result = asyncio.run(explainer.generate_portfolio_explanation(
            sample_current, sample_optimized, sample_historical, sample_context, "Balanced"
        ))
        print("Professional Advisory with Market Data Test:")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Test failed: {e}")


if __name__ == "__main__":
    test_professional_explainer()
