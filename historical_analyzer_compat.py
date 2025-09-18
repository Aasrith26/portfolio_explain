"""
Backward Compatibility Layer
Ensures existing code works unchanged with new hybrid system
"""
import asyncio
import logging
from flask_hybrid_integration import portfolio_api

logger = logging.getLogger(__name__)


class EnhancedHistoricalAnalyzer:
    """
    Drop-in replacement for original EnhancedHistoricalAnalyzer
    Routes all requests through hybrid system
    """

    def __init__(self):
        logger.info("🔄 Backward compatibility layer initialized")
        logger.info("📊 All requests will use hybrid data system")

    def calculate_all_metrics(self, portfolios):
        """
        Drop-in replacement for calculate_all_metrics
        Routes through hybrid system
        """
        return asyncio.run(self._async_calculate_all_metrics(portfolios))

    async def _async_calculate_all_metrics(self, portfolios):
        """Async implementation using hybrid system"""
        try:
            request_data = {
                'portfolios': portfolios,
                'include_live_data': True,
                'request_id': 'compatibility_layer'
            }

            result = await portfolio_api.handle_portfolio_request(request_data)

            if result.get('status') == 'success':
                portfolio_data = result.get('portfolio_data', {})

                # Convert to original format
                historical_metrics = {}
                for asset, data in portfolio_data.items():
                    historical_metrics[asset] = {
                        'asset_name': data.get('asset_name'),
                        'current_price': data.get('current_price', 0),
                        'data_source': data.get('data_source', 'hybrid_system'),
                        'last_update': data.get('last_update'),
                        'historical_returns': data.get('historical_returns', {}),
                        'risk_metrics': data.get('risk_metrics', {}),
                        'current_stats': data.get('current_stats', {}),
                        'data_quality': data.get('data_quality', {})
                    }

                logger.info(f"✅ Hybrid system provided data for {len(historical_metrics)} assets")
                return historical_metrics
            else:
                logger.error(f"❌ Hybrid system failed: {result.get('error')}")
                return self._get_emergency_fallback(portfolios)

        except Exception as e:
            logger.error(f"❌ Compatibility layer error: {e}")
            return self._get_emergency_fallback(portfolios)

    def _get_emergency_fallback(self, portfolios):
        """Emergency fallback data"""
        fallback_data = {}

        # Extract unique assets
        assets = set()
        for portfolio in portfolios.values():
            assets.update(portfolio.keys())

        # Provide basic fallback for each asset
        for asset in assets:
            fallback_data[asset] = self._get_asset_fallback(asset)

        return fallback_data

    def _get_asset_fallback(self, asset):
        """Get fallback data for single asset"""
        defaults = {
            'Equities': {'price': 25000, 'returns': {'1_year': '15.0%', '5_years_avg': '12.0%'},
                         'risk': {'volatility': '18.0%', 'sharpe_ratio': '0.95'}},
            'Gold': {'price': 65000, 'returns': {'1_year': '8.0%', '5_years_avg': '10.0%'},
                     'risk': {'volatility': '15.0%', 'sharpe_ratio': '1.05'}},
            'Bitcoin': {'price': 4500000, 'returns': {'1_year': '45.0%', '5_years_avg': '80.0%'},
                        'risk': {'volatility': '65.0%', 'sharpe_ratio': '0.85'}},
            'REITs': {'price': 180, 'returns': {'1_year': '12.0%', '5_years_avg': '15.0%'},
                      'risk': {'volatility': '22.0%', 'sharpe_ratio': '0.88'}}
        }

        asset_defaults = defaults.get(asset, defaults['Equities'])

        return {
            'asset_name': asset,
            'current_price': asset_defaults['price'],
            'data_source': 'emergency_fallback',
            'last_update': '2025-09-18T12:00:00',
            'historical_returns': asset_defaults['returns'],
            'risk_metrics': asset_defaults['risk'],
            'current_stats': {
                'current_price': asset_defaults['price'],
                'avg_annual_return': asset_defaults['returns']['1_year']
            },
            'data_quality': {'data_points': 0, 'completeness': 0}
        }

    def get_data_status(self):
        """Get data status from hybrid system"""
        try:
            return portfolio_api.get_system_status()
        except Exception as e:
            logger.error(f"❌ Error getting status: {e}")
            return {'error': str(e), 'system': 'hybrid_compatibility_layer'}

    def force_refresh_all(self):
        """Force refresh through hybrid system"""
        return asyncio.run(portfolio_api.refresh_system())


# Backward compatibility alias
HistoricalAnalyzer = EnhancedHistoricalAnalyzer