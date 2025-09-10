import os
import json
import logging
from openai import AzureOpenAI
from config import Config
from datetime import datetime

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
        """Generate portfolio explanation using enhanced live data - SAME OUTPUT FORMAT"""

        logger.info("🤖 Generating portfolio explanation with live market data (maintaining original format)")

        # Prepare enhanced prompt with live data
        prompt = self._create_enhanced_live_data_prompt(
            current_portfolio, optimized_portfolio,
            historical_metrics, context_data, risk_profile
        )

        try:
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a senior portfolio manager with decades of experience advising high-net-worth clients. You have access to live market data, real-time pricing, and comprehensive market analysis. Provide professional investment recommendations using the actual current market data provided. Use specific numbers, prices, and metrics to support your decisions. Speak with authority based on the real market conditions and data provided. MAINTAIN THE EXACT JSON STRUCTURE REQUESTED."
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
            logger.info(f"✅ LLM response received: {len(explanation_text)} characters")

            explanation_json = json.loads(explanation_text)

            # Add historical metrics to response (ORIGINAL METHOD - preserved exactly)
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

    def _create_enhanced_live_data_prompt(self, current_portfolio, optimized_portfolio,
                                          historical_metrics, context_data, risk_profile):
        """Create enhanced prompt with live market data BUT maintain original JSON format"""

        # Calculate portfolio changes with live data
        changes_analysis = self._create_detailed_changes_with_prices(current_portfolio, optimized_portfolio,
                                                                     historical_metrics)

        # Extract live market data for context
        live_market_context = self._extract_live_market_intelligence(historical_metrics, context_data)

        return f"""
You are providing investment advice based on LIVE MARKET DATA and comprehensive analysis. Use the actual current market prices and data provided.

CLIENT PORTFOLIO REBALANCING WITH LIVE DATA:
Current Allocation: {json.dumps(current_portfolio)}%
Recommended Allocation: {json.dumps(optimized_portfolio)}%
Risk Profile: {risk_profile}
Portfolio Changes Analysis: {changes_analysis}

LIVE MARKET DATA & INTELLIGENCE:
{live_market_context}

PROFESSIONAL ADVISORY GUIDELINES:
- Use the ACTUAL current market prices and data provided above
- Reference specific price levels, returns, and volatility figures from the live data
- Explain allocation decisions using real market conditions and performance metrics
- Include relevant current market prices, performance figures, and risk metrics in your explanations
- Make explanations data-driven using the live information provided
- Each asset explanation should be 5-6 sentences with supporting live market data

CRITICAL: MAINTAIN EXACT JSON STRUCTURE - DO NOT ADD OR CHANGE FIELD NAMES

PROFESSIONAL ADVISORY OUTPUT (Strict JSON - EXACT FORMAT):
{{
  "portfolio_analysis": {{
    "overall_explanation": "5-6 sentences explaining the overall investment strategy using live market data, current prices, and actual performance metrics that support the {risk_profile} allocation approach",
    "allocation_rationale": "4-5 sentences explaining the specific allocation percentages using current market prices, recent performance data, and live market conditions",

    "assets": {{
      "Equities": {{
        "allocation_pct": {optimized_portfolio.get('Equities', 0)},
        "change_from_current": "{self._get_precise_change('Equities', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining the equity allocation using live market data including current NIFTY levels (from live data), actual performance metrics, real volatility readings, and current market conditions. Reference the specific current price and actual performance data provided.",
        "key_market_data": {{
          "institutional_flows": "Actual FII/DII flow data and institutional activity metrics from current market analysis",
          "market_levels": "Current NIFTY50 levels and technical patterns from live market data", 
          "policy_metrics": "Current RBI repo rate, policy stance, and monetary policy impact from real data",
          "sentiment_indicators": "Actual market sentiment metrics and investor behavior indicators from live analysis",
          "global_context": "Real US market performance, DXY levels, and international market influences from current data"
        }}
      }},
      "Gold": {{
        "allocation_pct": {optimized_portfolio.get('Gold', 0)},
        "change_from_current": "{self._get_precise_change('Gold', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining gold allocation using live market data including current gold prices (from live data), actual performance metrics, real USD/INR levels, and current market conditions. Reference specific current prices and actual performance data provided.",
        "key_market_data": {{
          "institutional_flows": "Central bank gold purchase data and institutional precious metals activity from current analysis",
          "market_levels": "Current gold price levels and technical patterns from live market data",
          "policy_metrics": "Real interest rate environment and yield curve analysis from current data", 
          "sentiment_indicators": "Actual gold market sentiment and safe-haven demand indicators from live analysis",
          "global_context": "USD strength, inflation expectations, and global economic factors from current data"
        }}
      }},
      "Bitcoin": {{
        "allocation_pct": {optimized_portfolio.get('Bitcoin', 0)},
        "change_from_current": "{self._get_precise_change('Bitcoin', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining bitcoin allocation using live market data including current BTC price (from live data), actual performance metrics, real volatility readings, and current market momentum. Reference specific current price and actual performance data provided.",
        "key_market_data": {{
          "institutional_flows": "Institutional cryptocurrency adoption and investment flow data from current market analysis",
          "market_levels": "Current Bitcoin price levels and technical momentum indicators from live market data",
          "policy_metrics": "Regulatory environment and policy developments affecting cryptocurrency markets from current data", 
          "sentiment_indicators": "Actual crypto market sentiment and trader positioning indicators from live analysis",
          "global_context": "Global cryptocurrency adoption trends and macroeconomic factors from current data"
        }}
      }},
      "REITs": {{
        "allocation_pct": {optimized_portfolio.get('REITs', 0)},
        "change_from_current": "{self._get_precise_change('REITs', current_portfolio, optimized_portfolio)}",
        "explanation": "5-6 sentences explaining REIT allocation using live market data including current REIT prices (from live data), actual performance metrics, real dividend yields, and current market conditions. Reference specific current prices and actual performance data provided.",
        "key_market_data": {{
          "institutional_flows": "Institutional real estate investment activity and REIT fund flows from current market analysis",
          "market_levels": "Current REIT price levels and real estate market fundamentals from live market data",
          "policy_metrics": "Interest rate environment impact on REITs and property market policies from current data", 
          "sentiment_indicators": "Actual REIT market sentiment and income-seeking investor behavior from live analysis",
          "global_context": "Global real estate trends and comparative yield analysis from current data"
        }}
      }}
    }},

    "portfolio_level": {{
      "risk_profile": "{risk_profile}",
      "market_environment": "3-4 sentences describing current market conditions using live data including specific price levels, actual economic indicators, and real market metrics that influence portfolio positioning",
      "diversification_metrics": "3-4 sentences explaining how the allocation spreads risk using actual correlation data, current volatility measures, and real diversification benefits with specific current metrics", 
      "timing_analysis": "3-4 sentences explaining why current market levels, actual valuations, and real economic indicators make this an opportune time for rebalancing based on live data",
      "performance_outlook": "Your professional assessment of expected portfolio characteristics using current market data, actual valuations, and real market cycle positioning"
    }}
  }}
}}

CRITICAL REMINDER: Use the live market data provided to inform your explanations, but maintain the EXACT JSON structure above. Do not add new fields or change field names.
"""

    def _create_detailed_changes_with_prices(self, current_portfolio, optimized_portfolio, historical_metrics):
        """Create detailed analysis with actual market prices for prompt context"""
        changes = []

        for asset in optimized_portfolio:
            current_pct = current_portfolio.get(asset, 0)
            optimized_pct = optimized_portfolio[asset]
            change = optimized_pct - current_pct

            # Get live price data
            asset_data = historical_metrics.get(asset, {})
            current_price = asset_data.get('current_stats', {}).get('current_price', 0)
            returns_1y = asset_data.get('historical_returns', {}).get('1_year', 'N/A')
            volatility = asset_data.get('risk_metrics', {}).get('volatility', 'N/A')
            data_source = asset_data.get('data_source', 'unknown')

            change_desc = f"{asset}: {current_pct}% → {optimized_pct}% ({change:+.1f}%)"
            price_desc = f"Current Live Price: ${current_price:,.2f}, 1Y Return: {returns_1y}, Volatility: {volatility}"
            source_desc = f"Source: {data_source}"

            changes.append(f"{change_desc} | {price_desc} | {source_desc}")

        return " ; ".join(changes)

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

    def _extract_live_market_intelligence(self, historical_metrics, context_data):
        """Extract comprehensive live market intelligence for prompt context"""
        intelligence_sections = []

        for asset in ['Equities', 'Gold', 'Bitcoin', 'REITs']:
            # Get historical (live price) data
            historical_data = historical_metrics.get(asset, {})
            current_price = historical_data.get('current_stats', {}).get('current_price', 0)
            returns_1y = historical_data.get('historical_returns', {}).get('1_year', 'N/A')
            returns_5y = historical_data.get('historical_returns', {}).get('5_years_avg', 'N/A')
            volatility = historical_data.get('risk_metrics', {}).get('volatility', 'N/A')
            max_drawdown = historical_data.get('risk_metrics', {}).get('max_drawdown', 'N/A')
            sharpe_ratio = historical_data.get('risk_metrics', {}).get('sharpe_ratio', 'N/A')
            data_source = historical_data.get('data_source', 'unknown')
            last_update = historical_data.get('last_update', 'unknown')

            # Get context (sentiment) data
            asset_mapping = {'Equities': 'NIFTY50', 'Gold': 'GOLD', 'Bitcoin': 'BITCOIN', 'REITs': 'REIT'}
            context_key = asset_mapping.get(asset, asset)
            context_info = context_data.get(context_key, {})
            overall_sentiment = context_info.get('overall_sentiment', 0)
            sentiment_interpretation = context_info.get('sentiment_interpretation', 'NEUTRAL')
            indicators = context_info.get('key_indicators', {})

            if asset == 'Equities':
                intelligence_sections.append(f"""
LIVE INDIAN EQUITY MARKET DATA:
Current NIFTY50: {current_price:,.2f} (Source: {data_source})
Performance: 1Y: {returns_1y}, 5Y Avg: {returns_5y}, Volatility: {volatility}, Max DD: {max_drawdown}, Sharpe: {sharpe_ratio}
Market Sentiment: {sentiment_interpretation} ({overall_sentiment:.3f})
FII/DII Activity: {self._extract_indicator_insight(indicators.get('fii_dii_flows', {}))}
Technical Analysis: {self._extract_indicator_insight(indicators.get('technical_analysis', {}))}
Market Mood: {self._extract_indicator_insight(indicators.get('market_sentiment', {}))}
Policy Environment: {self._extract_indicator_insight(indicators.get('rbi_policy', {}))}
Global Factors: {self._extract_indicator_insight(indicators.get('global_factors', {}))}
Updated: {last_update[:19] if last_update != 'unknown' else 'Recently'}
                """)

            elif asset == 'Gold':
                intelligence_sections.append(f"""
LIVE GOLD MARKET DATA:
Current Gold Price: ${current_price:,.2f} (Source: {data_source})
Performance: 1Y: {returns_1y}, 5Y Avg: {returns_5y}, Volatility: {volatility}, Max DD: {max_drawdown}, Sharpe: {sharpe_ratio}
Market Sentiment: {sentiment_interpretation} ({overall_sentiment:.3f})
Price Momentum: {self._extract_indicator_insight(indicators.get('price_momentum', {}))}
USD/INR Impact: {self._extract_indicator_insight(indicators.get('usd_inr_impact', {}))}
Interest Rates: {self._extract_indicator_insight(indicators.get('interest_rate_impact', {}))}
Inflation Hedge: {self._extract_indicator_insight(indicators.get('inflation_indicators', {}))}
Central Banks: {self._extract_indicator_insight(indicators.get('global_sentiment', {}))}
Updated: {last_update[:19] if last_update != 'unknown' else 'Recently'}
                """)

            elif asset == 'Bitcoin':
                intelligence_sections.append(f"""
LIVE BITCOIN MARKET DATA:
Current BTC Price: ${current_price:,.2f} (Source: {data_source})
Performance: 1Y: {returns_1y}, 5Y Avg: {returns_5y}, Volatility: {volatility}, Max DD: {max_drawdown}, Sharpe: {sharpe_ratio}
Market Sentiment: {sentiment_interpretation} ({overall_sentiment:.3f})
Momentum: {self._extract_indicator_insight(indicators.get('micro_momentum', {}))}
Funding: {self._extract_indicator_insight(indicators.get('funding_rates', {}))}
Liquidity: {self._extract_indicator_insight(indicators.get('liquidity_analysis', {}))}
Order Flow: {self._extract_indicator_insight(indicators.get('order_flow', {}))}
Updated: {last_update[:19] if last_update != 'unknown' else 'Recently'}
                """)

            elif asset == 'REITs':
                intelligence_sections.append(f"""
LIVE REIT MARKET DATA:
Current REIT Price: ₹{current_price:,.2f} (Source: {data_source})
Performance: 1Y: {returns_1y}, 5Y Avg: {returns_5y}, Volatility: {volatility}, Max DD: {max_drawdown}, Sharpe: {sharpe_ratio}
Market Sentiment: {sentiment_interpretation} ({overall_sentiment:.3f})
Technical: {self._extract_indicator_insight(indicators.get('technical_momentum', {}))}
Yields: {self._extract_indicator_insight(indicators.get('yield_spread', {}))}
Flows: {self._extract_indicator_insight(indicators.get('accumulation_flow', {}))}
Liquidity: {self._extract_indicator_insight(indicators.get('liquidity_risk', {}))}
Updated: {last_update[:19] if last_update != 'unknown' else 'Recently'}
                """)

        return "\n".join(intelligence_sections)

    def _extract_indicator_insight(self, indicator_data):
        """Extract natural insight from indicator data"""
        description = indicator_data.get('description', 'Market conditions stable')
        sentiment = indicator_data.get('sentiment', 0)

        # Add context if significant
        if abs(sentiment) > 0.2:
            if sentiment > 0.2:
                context = " (positive momentum)"
            else:
                context = " (defensive positioning)"
            description += context

        return description

    def _add_historical_metrics_to_response(self, explanation_json, historical_metrics):
        """Add historical metrics to the LLM response after processing - ORIGINAL METHOD PRESERVED"""

        try:
            # Add historical returns and risk metrics to each asset
            assets = explanation_json.get('portfolio_analysis', {}).get('assets', {})

            for asset_name, asset_data in assets.items():
                if asset_name in historical_metrics:
                    # Add historical returns (ORIGINAL FORMAT)
                    asset_data['historical_returns'] = historical_metrics[asset_name].get('historical_returns', {})

                    # Add risk metrics (ORIGINAL FORMAT)
                    asset_data['risk_metrics'] = historical_metrics[asset_name].get('risk_metrics', {})

                    # Add current stats (ORIGINAL FORMAT)
                    asset_data['current_stats'] = historical_metrics[asset_name].get('current_stats', {})

            return explanation_json

        except Exception as e:
            logger.error(f"Error adding historical metrics: {e}")
            return explanation_json


# Test function (preserved)
def test_enhanced_llm_explainer():
    explainer = PortfolioExplainer()

    # Sample data
    sample_current = {"Equities": 30.0, "Gold": 40.0, "Bitcoin": 20.0, "REITs": 10.0}
    sample_optimized = {"Equities": 32.8, "Gold": 42.5, "Bitcoin": 10.0, "REITs": 14.7}

    # Sample historical data (mimicking live data structure)
    sample_historical = {
        "Equities": {
            "current_stats": {"current_price": 24973.10},
            "historical_returns": {"1_year": "0.5%", "5_years_avg": "12.8%", "10_years_avg": "11.2%"},
            "risk_metrics": {"volatility": "13.3%", "max_drawdown": "-18.5%", "sharpe_ratio": "0.95",
                             "var_95": "-2.1%"},
            "data_source": "direct_yfinance",
            "last_update": datetime.now().isoformat()
        },
        "Gold": {
            "current_stats": {"current_price": 3691.80},
            "historical_returns": {"1_year": "47.0%", "5_years_avg": "8.2%", "10_years_avg": "9.1%"},
            "risk_metrics": {"volatility": "16.3%", "max_drawdown": "-12.1%", "sharpe_ratio": "1.12",
                             "var_95": "-1.8%"},
            "data_source": "direct_yfinance",
            "last_update": datetime.now().isoformat()
        },
        "Bitcoin": {
            "current_stats": {"current_price": 112318.35},
            "historical_returns": {"1_year": "19.0%", "5_years_avg": "85.2%", "10_years_avg": "120.5%"},
            "risk_metrics": {"volatility": "37.0%", "max_drawdown": "-78.2%", "sharpe_ratio": "0.82",
                             "var_95": "-4.5%"},
            "data_source": "direct_yfinance",
            "last_update": datetime.now().isoformat()
        },
        "REITs": {
            "current_stats": {"current_price": 305.75},
            "historical_returns": {"1_year": "12.5%", "5_years_avg": "15.8%", "10_years_avg": "13.2%"},
            "risk_metrics": {"volatility": "22.1%", "max_drawdown": "-32.5%", "sharpe_ratio": "0.88",
                             "var_95": "-2.8%"},
            "data_source": "direct_yfinance",
            "last_update": datetime.now().isoformat()
        }
    }

    # Sample context data
    sample_context = {
        "NIFTY50": {
            "overall_sentiment": 0.245,
            "sentiment_interpretation": "BULLISH",
            "key_indicators": {
                "fii_dii_flows": {"description": "Strong institutional buying with ₹2,500 crore FII net purchases"},
                "technical_analysis": {"description": "Bullish breakout above 24,800 resistance level confirmed"}
            }
        }
    }

    try:
        import asyncio
        result = asyncio.run(explainer.generate_portfolio_explanation(
            sample_current, sample_optimized, sample_historical, sample_context, "Balanced"
        ))
        print("Enhanced LLM Test Result (PRESERVED JSON FORMAT):")
        print(json.dumps(result, indent=2))

        # Verify structure
        if 'portfolio_analysis' in result:
            portfolio_analysis = result['portfolio_analysis']
            print(f"\n✅ Structure Check:")
            print(f"   Has overall_explanation: {'overall_explanation' in portfolio_analysis}")
            print(f"   Has allocation_rationale: {'allocation_rationale' in portfolio_analysis}")
            print(f"   Has assets: {'assets' in portfolio_analysis}")
            print(f"   Has portfolio_level: {'portfolio_level' in portfolio_analysis}")

            if 'assets' in portfolio_analysis:
                bitcoin = portfolio_analysis['assets'].get('Bitcoin', {})
                print(f"   Bitcoin has historical_returns: {'historical_returns' in bitcoin}")
                print(f"   Bitcoin has risk_metrics: {'risk_metrics' in bitcoin}")
                print(f"   Bitcoin has current_stats: {'current_stats' in bitcoin}")
                print(f"   Bitcoin has key_market_data: {'key_market_data' in bitcoin}")

    except Exception as e:
        print(f"Enhanced LLM test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_enhanced_llm_explainer()
