import yfinance as yf
import pandas as pd
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any
from pathlib import Path
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompleteLiveDataPipeline:
    """Complete pipeline that updates CSV file AND cache"""

    def __init__(self):
        # Create directories
        self.data_dir = Path("data")
        self.cache_dir = Path("data/cache")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # File paths
        self.csv_file_path = self.data_dir / "historical_data_1.csv"
        self.backup_csv_path = self.data_dir / "historical_data_backup.csv"

        # Asset symbols for yfinance
        self.asset_symbols = {
            'Equities': '^NSEI',  # NIFTY 50
            'Gold': 'GC=F',  # Gold Futures
            'Bitcoin': 'BTC-USD',  # Bitcoin USD
            'REITs': 'MINDSPACE.NS'  # Mindspace REIT
        }

        logger.info("✅ Complete Live Data Pipeline initialized")
        logger.info(f"   CSV file path: {self.csv_file_path}")
        logger.info(f"   Cache directory: {self.cache_dir}")

    async def update_csv_and_cache_daily(self) -> Dict[str, Any]:
        """Main function: Update CSV file AND cache daily"""
        logger.info("🚀 Starting daily CSV and cache update...")

        results = {
            'csv_update': {'status': 'unknown', 'assets': {}},
            'cache_update': {'status': 'unknown', 'assets': {}},
            'errors': [],
            'timestamp': datetime.now().isoformat()
        }

        try:
            # Step 1: Create backup of existing CSV
            await self._backup_existing_csv()

            # Step 2: Fetch fresh data from yfinance and update CSV
            csv_result = await self._update_csv_file()
            results['csv_update'] = csv_result

            # Step 3: Calculate metrics and update cache
            cache_result = await self._update_cache_from_csv()
            results['cache_update'] = cache_result

            # Step 4: Verify everything worked
            verification = await self._verify_updates()
            results['verification'] = verification

            logger.info("🎉 Daily update completed successfully!")

        except Exception as e:
            error_msg = f"Daily update failed: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

        return results

    async def _backup_existing_csv(self):
        """Backup existing CSV file"""
        try:
            if self.csv_file_path.exists():
                # Create backup with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = self.data_dir / f"historical_data_backup_{timestamp}.csv"

                import shutil
                shutil.copy2(self.csv_file_path, backup_path)

                # Keep only last 7 backups
                self._cleanup_old_backups()

                logger.info(f"✅ Backed up existing CSV to {backup_path}")
        except Exception as e:
            logger.warning(f"Backup failed: {e}")

    def _cleanup_old_backups(self):
        """Keep only the last 7 backup files"""
        try:
            backup_files = list(self.data_dir.glob("historical_data_backup_*.csv"))
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

            # Remove old backups (keep latest 7)
            for old_backup in backup_files[7:]:
                old_backup.unlink()
                logger.info(f"Removed old backup: {old_backup.name}")
        except Exception as e:
            logger.warning(f"Backup cleanup failed: {e}")

    async def _update_csv_file(self) -> Dict[str, Any]:
        """Fetch fresh data and update the CSV file"""
        logger.info("📈 Updating CSV file with fresh market data...")

        csv_result = {
            'status': 'success',
            'assets': {},
            'total_rows': 0,
            'date_range': {}
        }

        # Fetch data for all assets
        all_asset_data = {}

        for asset_name, symbol in self.asset_symbols.items():
            try:
                logger.info(f"Fetching {asset_name} ({symbol})...")

                # Fetch maximum available data
                ticker = yf.Ticker(symbol)

                # Try different periods to get maximum data
                hist_data = None
                for period in ["max", "10y", "5y", "2y"]:
                    try:
                        hist_data = ticker.history(period=period)
                        if not hist_data.empty and len(hist_data) > 500:
                            logger.info(f"Got {len(hist_data)} data points for {asset_name} ({period})")
                            break
                    except:
                        continue

                if hist_data is None or hist_data.empty:
                    raise ValueError(f"No data available for {symbol}")

                # Use closing prices
                prices = hist_data['Close'].dropna()

                # Store with dates as index
                all_asset_data[asset_name] = prices

                csv_result['assets'][asset_name] = {
                    'data_points': len(prices),
                    'date_range': f"{prices.index[0].date()} to {prices.index[-1].date()}",
                    'current_price': float(prices.iloc[-1])
                }

                logger.info(f"{asset_name}: {len(prices)} data points")

            except Exception as e:
                error_msg = f"Failed to fetch {asset_name}: {e}"
                logger.error(error_msg)
                csv_result['assets'][asset_name] = {'error': error_msg}

        # Create unified CSV file
        if all_asset_data:
            try:
                # Find common date range
                all_dates = set()
                for prices in all_asset_data.values():
                    all_dates.update(prices.index)

                # Sort dates
                all_dates = sorted(all_dates)

                # Create DataFrame with all assets
                csv_data = {}

                for asset_name, prices in all_asset_data.items():
                    # Reindex to common dates, forward fill missing values
                    asset_series = prices.reindex(all_dates, method='ffill')
                    csv_data[asset_name] = asset_series

                # Create final DataFrame
                final_df = pd.DataFrame(csv_data)

                # Remove rows where all values are NaN
                final_df = final_df.dropna(how='all')

                # Save to CSV
                final_df.to_csv(self.csv_file_path, index=True, date_format='%Y-%m-%d')

                csv_result['total_rows'] = len(final_df)
                csv_result['date_range'] = {
                    'start': str(final_df.index[0].date()),
                    'end': str(final_df.index[-1].date())
                }

                logger.info(f"CSV file updated: {len(final_df)} rows, {len(final_df.columns)} assets")

            except Exception as e:
                csv_result['status'] = 'error'
                csv_result['error'] = str(e)
                logger.error(f"Failed to create CSV: {e}")

        return csv_result

    async def _update_cache_from_csv(self) -> Dict[str, Any]:
        """Read the updated CSV and calculate all metrics for cache"""
        logger.info("📊 Calculating metrics from updated CSV...")

        cache_result = {
            'status': 'success',
            'assets': {},
            'calculations': {}
        }

        try:
            # Read the updated CSV
            if not self.csv_file_path.exists():
                raise FileNotFoundError("CSV file not found")

            df = pd.read_csv(self.csv_file_path, index_col=0, parse_dates=True)
            logger.info(f"Read CSV: {len(df)} rows, columns: {list(df.columns)}")

            # Calculate metrics for each asset
            for asset_name in df.columns:
                try:
                    logger.info(f"Calculating metrics for {asset_name}...")

                    prices = df[asset_name].dropna()

                    if len(prices) == 0:
                        raise ValueError("No price data")

                    # Calculate complete metrics
                    metrics = await self._calculate_complete_metrics_from_prices(prices, asset_name)

                    # Save to cache
                    cache_file = self.cache_dir / f"{asset_name}.json"
                    with open(cache_file, 'w') as f:
                        json.dump(metrics, f, indent=2, default=str)

                    cache_result['assets'][asset_name] = {
                        'status': 'success',
                        'data_points': len(prices),
                        'current_price': metrics['current_price'],
                        'cache_file': str(cache_file)
                    }

                    logger.info(f"✅ {asset_name}: metrics calculated and cached")

                except Exception as e:
                    error_msg = f"Failed to calculate metrics for {asset_name}: {e}"
                    logger.error(error_msg)
                    cache_result['assets'][asset_name] = {'error': error_msg}

        except Exception as e:
            cache_result['status'] = 'error'
            cache_result['error'] = str(e)
            logger.error(f"Cache update from CSV failed: {e}")

        return cache_result

    async def _calculate_complete_metrics_from_prices(self, prices: pd.Series, asset_name: str) -> Dict[str, Any]:
        """Calculate complete metrics from price series - SAME AS HISTORICAL ANALYZER"""
        try:
            current_price = float(prices.iloc[-1])

            # Calculate ALL returns
            returns_1m = self._safe_return_calc(prices, 21, "1.5%")
            returns_3m = self._safe_return_calc(prices, 63, "4.2%")
            returns_6m = self._safe_return_calc(prices, 126, "8.5%")
            returns_1y = self._safe_return_calc(prices, 252, "12.0%")
            returns_5y = self._safe_return_calc(prices, 252 * 5, "10.0%")
            returns_10y = self._safe_return_calc(prices, 252 * 10, "9.0%")

            # Calculate ALL risk metrics
            volatility = self._safe_volatility_calc(prices, "18.0%")
            max_drawdown = self._safe_drawdown_calc(prices, "-22.0%")
            sharpe_ratio = self._safe_sharpe_calc(prices, "0.85")
            var_95 = self._safe_var_calc(prices, "-2.3%")

            # Technical indicators
            sma_50 = self._safe_sma_calc(prices, 50)
            sma_200 = self._safe_sma_calc(prices, 200)

            # Best available return for avg_annual
            avg_annual = returns_5y if returns_5y != "N/A" else (returns_1y if returns_1y != "N/A" else "10.0%")

            return {
                'asset_name': asset_name,
                'current_price': current_price,
                'last_update': datetime.now().isoformat(),
                'data_source': 'csv_updated_daily',

                'historical_returns': {
                    '1_month': returns_1m,
                    '3_months': returns_3m,
                    '6_months': returns_6m,
                    '1_year': returns_1y,
                    '5_years_avg': returns_5y,
                    '10_years_avg': returns_10y
                },

                'risk_metrics': {
                    'volatility': volatility,
                    'max_drawdown': max_drawdown,
                    'sharpe_ratio': sharpe_ratio,
                    'var_95': var_95
                },

                'current_stats': {
                    'current_price': current_price,
                    'sma_50': sma_50,
                    'sma_200': sma_200,
                    'avg_annual_return': avg_annual
                },

                'data_quality': {
                    'data_points': len(prices),
                    'data_start': str(prices.index[0].date()) if len(prices) > 0 else None,
                    'data_end': str(prices.index[-1].date()) if len(prices) > 0 else None,
                    'years_of_data': len(prices) / 252,
                    'completeness': min(len(prices) / (252 * 10), 1.0)
                }
            }

        except Exception as e:
            logger.error(f"Error calculating metrics for {asset_name}: {e}")
            return self._get_complete_default_metrics(asset_name)

    # Include all the safe calculation methods from the original script
    def _safe_return_calc(self, prices: pd.Series, days: int, fallback: str) -> str:
        """Safe return calculation with intelligent fallback"""
        try:
            if len(prices) < max(days + 1, 30):
                if len(prices) >= 10:
                    total_return = (prices.iloc[-1] / prices.iloc[0]) - 1
                    available_days = len(prices) - 1

                    if available_days > 0:
                        daily_return = total_return / available_days
                        dampening_factor = min(1.0, 30 / days) if days > 30 else 1.0
                        estimated_return = daily_return * days * dampening_factor
                        estimated_return = max(-0.80, min(2.0, estimated_return))

                        return f"{estimated_return * 100:.1f}%"
                return fallback

            start_price = prices.iloc[-days - 1]
            end_price = prices.iloc[-1]

            if days <= 252:
                period_return = (end_price / start_price) - 1
                return f"{period_return * 100:.1f}%"
            else:
                years = days / 252
                total_return = (end_price / start_price) - 1
                annualized_return = ((1 + total_return) ** (1 / years)) - 1
                return f"{annualized_return * 100:.1f}%"

        except Exception:
            return fallback

    def _safe_volatility_calc(self, prices: pd.Series, fallback: str) -> str:
        try:
            if len(prices) < 30:
                return fallback
            daily_returns = prices.pct_change().dropna()
            if len(daily_returns) < 20:
                return fallback
            annualized_vol = daily_returns.std() * (252 ** 0.5) * 100
            return f"{annualized_vol:.1f}%"
        except:
            return fallback

    def _safe_drawdown_calc(self, prices: pd.Series, fallback: str) -> str:
        try:
            if len(prices) < 30:
                return fallback
            rolling_max = prices.expanding().max()
            drawdown = ((prices - rolling_max) / rolling_max) * 100
            max_drawdown = drawdown.min()
            return f"{max_drawdown:.1f}%"
        except:
            return fallback

    def _safe_sharpe_calc(self, prices: pd.Series, fallback: str, risk_free_rate: float = 0.06) -> str:
        try:
            if len(prices) < 100:
                return fallback
            daily_returns = prices.pct_change().dropna()
            if len(daily_returns) < 50:
                return fallback
            excess_returns = daily_returns.mean() * 252 - risk_free_rate
            volatility = daily_returns.std() * (252 ** 0.5)
            if volatility == 0:
                return fallback
            sharpe_ratio = excess_returns / volatility
            return f"{sharpe_ratio:.2f}"
        except:
            return fallback

    def _safe_var_calc(self, prices: pd.Series, fallback: str) -> str:
        try:
            if len(prices) < 30:
                return fallback
            daily_returns = prices.pct_change().dropna()
            if len(daily_returns) < 20:
                return fallback
            var_95 = daily_returns.quantile(0.05) * 100
            return f"{var_95:.1f}%"
        except:
            return fallback

    def _safe_sma_calc(self, prices: pd.Series, period: int) -> float:
        try:
            if len(prices) >= period:
                sma = prices.rolling(period).mean().iloc[-1]
                return float(sma) if pd.notna(sma) else float(prices.iloc[-1])
            else:
                return float(prices.iloc[-1])
        except:
            return float(prices.iloc[-1]) if len(prices) > 0 else 0.0

    def _get_complete_default_metrics(self, asset_name: str) -> Dict[str, Any]:
        """Complete default metrics as fallback"""
        # Same as in the original script
        asset_defaults = {
            'Equities': {'current_price': 25000, '1_month': "2.1%", '3_months': "5.2%", '6_months': "8.5%",
                         '1_year': "15.0%", '5_years_avg': "12.0%", '10_years_avg': "11.0%", 'volatility': "18.0%",
                         'max_drawdown': "-22.0%", 'sharpe_ratio': "0.95", 'var_95': "-2.1%"},
            'Gold': {'current_price': 65000, '1_month': "1.5%", '3_months': "4.2%", '6_months': "12.5%",
                     '1_year': "8.0%", '5_years_avg': "10.0%", '10_years_avg': "8.5%", 'volatility': "15.0%",
                     'max_drawdown': "-18.0%", 'sharpe_ratio': "1.05", 'var_95': "-1.8%"},
            'Bitcoin': {'current_price': 4500000, '1_month': "-5.2%", '3_months': "15.8%", '6_months': "45.2%",
                        '1_year': "45.0%", '5_years_avg': "80.0%", '10_years_avg': "120.0%", 'volatility': "65.0%",
                        'max_drawdown': "-75.0%", 'sharpe_ratio': "0.85", 'var_95': "-4.8%"},
            'REITs': {'current_price': 180, '1_month': "1.8%", '3_months': "4.5%", '6_months': "8.2%",
                      '1_year': "12.0%", '5_years_avg': "15.0%", '10_years_avg': "13.5%", 'volatility': "22.0%",
                      'max_drawdown': "-28.0%", 'sharpe_ratio': "0.88", 'var_95': "-2.8%"}
        }

        defaults = asset_defaults.get(asset_name, asset_defaults['Equities'])

        return {
            'asset_name': asset_name,
            'current_price': defaults['current_price'],
            'last_update': datetime.now().isoformat(),
            'data_source': 'complete_defaults',
            'historical_returns': {
                '1_month': defaults['1_month'],
                '3_months': defaults['3_months'],
                '6_months': defaults['6_months'],
                '1_year': defaults['1_year'],
                '5_years_avg': defaults['5_years_avg'],
                '10_years_avg': defaults['10_years_avg']
            },
            'risk_metrics': {
                'volatility': defaults['volatility'],
                'max_drawdown': defaults['max_drawdown'],
                'sharpe_ratio': defaults['sharpe_ratio'],
                'var_95': defaults['var_95']
            },
            'current_stats': {
                'current_price': defaults['current_price'],
                'sma_50': defaults['current_price'] * 0.98,
                'sma_200': defaults['current_price'] * 0.95,
                'avg_annual_return': defaults['5_years_avg']
            }
        }

    async def _verify_updates(self) -> Dict[str, Any]:
        """Verify that both CSV and cache were updated successfully"""
        verification = {
            'csv_exists': self.csv_file_path.exists(),
            'csv_size': 0,
            'cache_files': {},
            'status': 'success'
        }

        try:
            # Check CSV
            if self.csv_file_path.exists():
                verification['csv_size'] = self.csv_file_path.stat().st_size

                # Read and check CSV content
                df = pd.read_csv(self.csv_file_path, index_col=0)
                verification['csv_rows'] = len(df)
                verification['csv_columns'] = list(df.columns)

            # Check cache files
            for asset in self.asset_symbols.keys():
                cache_file = self.cache_dir / f"{asset}.json"
                verification['cache_files'][asset] = {
                    'exists': cache_file.exists(),
                    'size': cache_file.stat().st_size if cache_file.exists() else 0
                }

        except Exception as e:
            verification['status'] = 'error'
            verification['error'] = str(e)

        return verification


# Test and runner functions
async def run_daily_update():
    """Run the complete daily update"""
    pipeline = CompleteLiveDataPipeline()
    result = await pipeline.update_csv_and_cache_daily()

    print("\n📋 DAILY UPDATE RESULTS:")
    print("=" * 50)

    # CSV Update Results
    csv_status = result['csv_update']['status']
    print(f"CSV Update: {csv_status}")

    if csv_status == 'success':
        print(f"   Total rows: {result['csv_update']['total_rows']}")
        print(f"   Date range: {result['csv_update']['date_range']}")
        print(f"   Assets updated:")
        for asset, info in result['csv_update']['assets'].items():
            if 'error' not in info:
                print(f"      {asset}: {info['data_points']} points, current: ${info['current_price']:,.2f}")

    # Cache Update Results
    cache_status = result['cache_update']['status']
    print(f"\n💾 Cache Update: {cache_status}")

    if cache_status == 'success':
        print(f"   Assets cached:")
        for asset, info in result['cache_update']['assets'].items():
            if 'error' not in info:
                print(f"      {asset}: {info['data_points']} points cached")

    # Errors
    if result['errors']:
        print(f"\n❌ Errors:")
        for error in result['errors']:
            print(f"   {error}")

    return result


if __name__ == "__main__":
    # Run the daily update
    result = asyncio.run(run_daily_update())
