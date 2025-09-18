"""
FIXED Flask Hybrid Integration
Corrects the data source counting issue
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List
from hybrid_data_fetcher import AutoDownloadingHybridDataManager, AssetDataResult

logger = logging.getLogger(__name__)


class PortfolioAPI:
    """FIXED Portfolio-focused API for hybrid data system"""

    def __init__(self):
        self.hybrid_manager = AutoDownloadingHybridDataManager()
        self.request_counter = 0

        logger.info("📊 Portfolio API initialized with auto-downloading hybrid data system")

    async def handle_portfolio_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        FIXED: Handle portfolio request with proper data source counting
        Compatible with existing Flask app expectations
        """
        try:
            self.request_counter += 1
            request_id = request_data.get('request_id', f"req_{self.request_counter}")

            logger.info(f"📋 Processing portfolio request {request_id}")

            # Extract portfolios
            portfolios = request_data.get('portfolios', {})
            include_live = request_data.get('include_live_data', True)

            # Get all unique assets from portfolios
            all_assets = set()
            for portfolio_name, weights in portfolios.items():
                all_assets.update(weights.keys())

            assets_list = list(all_assets)
            logger.info(f"🎯 Assets needed: {assets_list}")

            # Get hybrid data for all assets
            start_time = datetime.now()
            asset_data = await self.hybrid_manager.get_portfolio_data(assets_list, request_id)
            fetch_time = (datetime.now() - start_time).total_seconds()

            # FIXED: Process results with correct source counting
            portfolio_data = {}
            data_sources = {'historical_count': 0, 'live_count': 0, 'cache_hits': 0, 'fallback_count': 0}

            for asset, result in asset_data.items():
                # Convert AssetDataResult to dict format expected by Flask app
                portfolio_data[asset] = self._convert_result_to_dict(result)

                # FIXED: Track data sources for analytics
                source = result.data_source
                logger.info(f"📊 {asset} source analysis: '{source}'")

                # Count historical sources
                if any(hist_indicator in source for hist_indicator in [
                    'csv_historical', 'auto_downloaded_csv', 'historical_csv', 'hybrid_auto_downloaded_csv'
                ]):
                    data_sources['historical_count'] += 1
                    logger.info(f"✅ {asset}: Counted as historical source")

                # Count live sources
                if any(live_indicator in source for live_indicator in [
                    'live', 'yfinance', 'today', 'hybrid_'
                ]):
                    data_sources['live_count'] += 1
                    logger.info(f"⚡ {asset}: Counted as live source")

                # Count cache hits
                if 'cache' in source:
                    data_sources['cache_hits'] += 1

                # Count fallbacks
                if 'fallback' in source:
                    data_sources['fallback_count'] += 1

            # Calculate portfolio metrics
            portfolio_metrics = self._calculate_portfolio_metrics(portfolios, portfolio_data)

            # FIXED: Prepare response with correct counting
            response = {
                'status': 'success',
                'request_id': request_id,
                'portfolio_data': portfolio_data,
                'portfolio_metrics': portfolio_metrics,
                'data_strategy': {
                    'type': 'hybrid_historical_plus_live',
                    'description': 'Auto-downloaded historical (X-1) + Live prices (day X)',
                    'historical_count': data_sources['historical_count'],
                    'live_count': data_sources['live_count'],
                    'cache_hits': data_sources['cache_hits'],
                    'fallback_count': data_sources['fallback_count'],
                    'total_requests': len(assets_list),
                    'concurrent_handled': 1,
                    'fetch_time_seconds': round(fetch_time, 2)
                },
                'metadata': {
                    'assets_processed': len(asset_data),
                    'portfolios_analyzed': len(portfolios),
                    'timestamp': datetime.now().isoformat(),
                    'hybrid_strategy_active': True,
                    'auto_download_enabled': True
                }
            }

            logger.info(f"✅ Portfolio request {request_id} completed in {fetch_time:.2f}s")
            logger.info(f"📊 FIXED Data sources: {data_sources}")

            return response

        except Exception as e:
            logger.error(f"❌ Portfolio request failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'request_id': request_data.get('request_id', 'unknown'),
                'timestamp': datetime.now().isoformat()
            }

    def _convert_result_to_dict(self, result: AssetDataResult) -> Dict[str, Any]:
        """Convert AssetDataResult to dict format expected by Flask app"""
        return {
            'asset_name': result.asset_name,
            'current_price': result.current_price,
            'data_source': result.data_source,
            'last_update': result.last_update,
            'historical_returns': result.historical_returns,
            'risk_metrics': result.risk_metrics,
            'current_stats': result.current_stats,
            'data_quality': result.data_quality,
            'error': result.error
        }

    def _calculate_portfolio_metrics(self, portfolios: Dict[str, Dict[str, float]],
                                   asset_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate portfolio-level metrics"""
        portfolio_metrics = {}

        for portfolio_name, weights in portfolios.items():
            try:
                total_value = 0
                weighted_returns = {'1_year': 0, '5_years_avg': 0}
                risk_metrics = {'volatility': 0, 'max_drawdown': 0}

                for asset, weight in weights.items():
                    if asset in asset_data:
                        asset_info = asset_data[asset]
                        asset_price = asset_info.get('current_price', 0)

                        # Calculate weighted contribution
                        weight_decimal = weight / 100
                        total_value += asset_price * weight_decimal

                        # Weighted returns (parse percentage strings)
                        returns = asset_info.get('historical_returns', {})
                        for period in ['1_year', '5_years_avg']:
                            return_str = returns.get(period, '0%')
                            return_val = float(return_str.replace('%', '')) / 100
                            weighted_returns[period] += return_val * weight_decimal

                        # Risk metrics (basic approximation)
                        risk = asset_info.get('risk_metrics', {})
                        vol_str = risk.get('volatility', '0%')
                        vol_val = float(vol_str.replace('%', '')) / 100
                        risk_metrics['volatility'] += vol_val * weight_decimal

                portfolio_metrics[portfolio_name] = {
                    'total_value': round(total_value, 2),
                    'weighted_returns': {
                        '1_year': f"{weighted_returns['1_year'] * 100:.1f}%",
                        '5_years_avg': f"{weighted_returns['5_years_avg'] * 100:.1f}%"
                    },
                    'risk_metrics': {
                        'portfolio_volatility': f"{risk_metrics['volatility'] * 100:.1f}%"
                    },
                    'asset_allocation': weights
                }

            except Exception as e:
                logger.error(f"❌ Error calculating metrics for portfolio {portfolio_name}: {e}")
                portfolio_metrics[portfolio_name] = {
                    'error': str(e),
                    'asset_allocation': weights
                }

        return portfolio_metrics

    async def get_asset_data(self, asset: str) -> Dict[str, Any]:
        """Get data for single asset"""
        try:
            logger.info(f"📊 Getting data for single asset: {asset}")

            result = await self.hybrid_manager._get_hybrid_asset_data(asset)
            return self._convert_result_to_dict(result)

        except Exception as e:
            logger.error(f"❌ Error getting data for {asset}: {e}")
            return {'error': str(e), 'asset': asset}

    async def get_live_prices(self, assets: List[str]) -> Dict[str, float]:
        """Get live prices for multiple assets"""
        try:
            logger.info(f"⚡ Getting live prices for: {assets}")

            live_prices = {}
            for asset in assets:
                price = await self.hybrid_manager._get_live_price_today(asset)
                if price:
                    live_prices[asset] = price
                else:
                    logger.warning(f"⚠️ No live price for {asset}")

            return live_prices

        except Exception as e:
            logger.error(f"❌ Error getting live prices: {e}")
            return {}

    async def refresh_system(self) -> Dict[str, Any]:
        """Refresh hybrid system"""
        try:
            logger.info("🔄 Refreshing auto-downloading hybrid system")
            return await self.hybrid_manager.force_refresh_all()
        except Exception as e:
            logger.error(f"❌ System refresh failed: {e}")
            return {'error': str(e)}

    def get_system_status(self) -> Dict[str, Any]:
        """Get system status"""
        try:
            return self.hybrid_manager.get_system_status()
        except Exception as e:
            logger.error(f"❌ Error getting system status: {e}")
            return {'error': str(e)}


# Create global instance for use by Flask app
portfolio_api = PortfolioAPI()


# Utility functions for backward compatibility
async def get_portfolio_explanation_data(portfolios: Dict[str, Dict[str, float]],
                                       request_id: str = None) -> Dict[str, Any]:
    """
    Backward compatible function for existing Flask app
    Returns data in format expected by historical_analyzer
    """
    try:
        request_data = {
            'portfolios': portfolios,
            'include_live_data': True,
            'request_id': request_id
        }

        result = await portfolio_api.handle_portfolio_request(request_data)

        if result.get('status') == 'success':
            # Convert to format expected by existing Flask app
            portfolio_data = result.get('portfolio_data', {})

            # Map to historical_analyzer format
            historical_metrics = {}
            for asset, data in portfolio_data.items():
                historical_metrics[asset] = {
                    'asset_name': data.get('asset_name'),
                    'current_price': data.get('current_price'),
                    'data_source': data.get('data_source'),
                    'last_update': data.get('last_update'),
                    'historical_returns': data.get('historical_returns', {}),
                    'risk_metrics': data.get('risk_metrics', {}),
                    'current_stats': data.get('current_stats', {}),
                    'data_quality': data.get('data_quality', {})
                }

            return historical_metrics
        else:
            logger.error(f"❌ Portfolio data fetch failed: {result.get('error')}")
            return {}

    except Exception as e:
        logger.error(f"❌ Error in backward compatibility function: {e}")
        return {}


async def calculate_all_metrics(portfolios: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    """
    Backward compatible function that mimics historical_analyzer.calculate_all_metrics()
    """
    return await get_portfolio_explanation_data(portfolios)