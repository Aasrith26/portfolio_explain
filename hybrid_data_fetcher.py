"""
Auto-Downloading Hybrid Data Manager
- First checks for CSV file
- If missing, automatically downloads 10 years of historical data from YFinance
- Creates the CSV file for future use
- Then combines historical + live data as before
"""
import asyncio
import json
import logging
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
import concurrent.futures
from dataclasses import dataclass
import hashlib
import os
logger = logging.getLogger(__name__)

@dataclass
class AssetDataResult:
    """Result container for asset data"""
    asset_name: str
    current_price: float
    data_source: str
    historical_returns: Dict[str, str]
    risk_metrics: Dict[str, str]
    current_stats: Dict[str, Any]
    data_quality: Dict[str, Any]
    last_update: str
    error: Optional[str] = None


class AutoDownloadingHybridDataManager:
    """
    AUTO-DOWNLOADING Hybrid Data Management System
    - Historical data: Auto-downloads from YFinance if CSV missing
    - Live data: YFinance for day X only (today's price updates)
    - Smart caching: Avoid duplicate requests
    - Self-initializing: Creates CSV on first run
    """

    def __init__(self):
        # File paths
        self.data_dir = Path("data")
        self.cache_dir = Path("data/cache")
        self.csv_file = Path("data/historical_data_1.csv")

        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Asset symbols for YFinance (same as your existing system)
        self.asset_symbols = {
            'Equities': '^NSEI',     # NIFTY 50
            'Gold': 'GC=F',          # Gold Futures
            'Bitcoin': 'BTC-USD',    # Bitcoin USD
            'REITs': 'MINDSPACE.NS'  # Mindspace REIT
        }

        # Request tracking for deduplication
        self.active_requests = {}
        self.request_lock = asyncio.Lock()

        # Cache for today's live data (expires at end of day)
        self.live_data_cache = {}
        self.live_cache_date = None

        # CSV download tracking (prevent multiple simultaneous downloads)
        self.csv_download_in_progress = False
        self.csv_download_lock = asyncio.Lock()

        logger.info("🚀 Auto-Downloading Hybrid Data Manager initialized")
        logger.info(f"📂 Data directory: {self.data_dir}")
        logger.info(f"📊 Historical CSV: {self.csv_file}")
        logger.info(f"⚡ Live data: YFinance (day X only)")
        logger.info(f"🎯 Strategy: Auto-download historical + Live pricing")

    async def get_portfolio_data(self, assets: List[str], request_id: str = None) -> Dict[str, AssetDataResult]:
        """
        Get hybrid portfolio data for multiple assets
        Auto-downloads historical data if CSV is missing
        """
        logger.info(f"🔄 Getting hybrid portfolio data for {len(assets)} assets")

        # Step 1: Ensure CSV exists (auto-download if missing)
        csv_ready = await self._ensure_csv_exists()
        if not csv_ready:
            logger.warning("⚠️ Could not create/download CSV file, will use live data + estimates")

        # Step 2: Generate request hash for deduplication
        request_hash = self._generate_request_hash(assets, request_id)

        # Step 3: Check for duplicate requests
        async with self.request_lock:
            if request_hash in self.active_requests:
                logger.info(f"⏳ Duplicate request detected, waiting for existing request")
                return await self.active_requests[request_hash]

            # Create future for this request
            future = asyncio.create_task(self._fetch_portfolio_data(assets))
            self.active_requests[request_hash] = future

        try:
            # Execute the request
            result = await future
            logger.info(f"✅ Portfolio data fetched: {len(result)} assets")
            return result
        finally:
            # Cleanup request tracking
            async with self.request_lock:
                self.active_requests.pop(request_hash, None)

    async def _ensure_csv_exists(self) -> bool:
        """
        Ensure CSV file exists - auto-download if missing
        Returns True if CSV is ready, False if failed
        """
        # Check if CSV already exists
        if self.csv_file.exists():
            # Verify it has data
            try:
                df = pd.read_csv(self.csv_file)
                if len(df) > 0 and len(df.columns) > 1:
                    logger.info(f"✅ CSV file exists with {len(df)} rows, {len(df.columns)} columns")
                    return True
                else:
                    logger.warning(f"⚠️ CSV file exists but empty, will re-download")
            except Exception as e:
                logger.warning(f"⚠️ CSV file corrupted ({e}), will re-download")

        # Need to download CSV
        logger.info(f"📥 CSV file missing, starting auto-download...")

        # Prevent multiple simultaneous downloads
        async with self.csv_download_lock:
            if self.csv_download_in_progress:
                logger.info(f"⏳ CSV download already in progress, waiting...")
                # Wait for other download to complete
                while self.csv_download_in_progress:
                    await asyncio.sleep(1)
                return self.csv_file.exists()

            # Start download
            self.csv_download_in_progress = True

        try:
            success = await self._download_and_create_csv()
            return success
        finally:
            self.csv_download_in_progress = False

    async def _download_and_create_csv(self) -> bool:
        """
        Download historical data and create CSV file
        Based on your existing daily_updater_no_redis.py logic
        """
        logger.info("🌐 Starting historical data download for all assets...")

        try:
            # Download data for all assets concurrently
            download_tasks = []
            for asset, symbol in self.asset_symbols.items():
                task = self._download_asset_historical_data(asset, symbol)
                download_tasks.append(task)

            # Execute all downloads concurrently
            results = await asyncio.gather(*download_tasks, return_exceptions=True)

            # Process results
            asset_data = {}
            for i, result in enumerate(results):
                asset = list(self.asset_symbols.keys())[i]
                if isinstance(result, Exception):
                    logger.error(f"❌ Failed to download {asset}: {result}")
                    continue
                elif result is not None and not result.empty:
                    asset_data[asset] = result['Close']
                    logger.info(f"✅ Downloaded {asset}: {len(result)} data points")
                else:
                    logger.warning(f"⚠️ No data downloaded for {asset}")

            if len(asset_data) == 0:
                logger.error("❌ No historical data downloaded for any asset")
                return False

            # Create unified CSV file
            success = await self._create_csv_from_data(asset_data)
            return success

        except Exception as e:
            logger.error(f"❌ Error during CSV download: {e}")
            return False

    async def _download_asset_historical_data(self, asset: str, symbol: str) -> Optional[pd.DataFrame]:
        """
        Download historical data for single asset
        Uses your existing YFinance approach with multiple period fallbacks
        """
        try:
            logger.info(f"📥 Downloading historical data for {asset} ({symbol})...")

            # Run YFinance download in executor (blocking operation)
            loop = asyncio.get_event_loop()
            hist_data = await loop.run_in_executor(None, self._fetch_yfinance_historical, symbol)

            return hist_data

        except Exception as e:
            logger.error(f"❌ Error downloading {asset}: {e}")
            return None

    def _fetch_yfinance_historical(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Fetch historical data from YFinance with multiple period fallbacks
        Based on your existing logic in daily_updater_no_redis.py
        """
        try:
            ticker = yf.Ticker(symbol)

            # Try different periods to get maximum data (your existing approach)
            for period_attempt in ["max", "10y", "5y", "2y", "1y"]:
                try:
                    logger.info(f"🔄 Trying {symbol} with period: {period_attempt}")
                    hist = ticker.history(period=period_attempt)

                    if not hist.empty and len(hist) > 100:  # Ensure we have enough data
                        logger.info(f"✅ Fetched {len(hist)} data points for {symbol} ({period_attempt} period)")
                        return hist

                except Exception as period_error:
                    logger.warning(f"⚠️ Period {period_attempt} failed for {symbol}: {period_error}")
                    continue

            logger.error(f"❌ No data found for {symbol} with any period")
            return None

        except Exception as e:
            logger.error(f"❌ YFinance fetch failed for {symbol}: {e}")
            return None

    """
    FIXED CSV Creation Method - Properly Formatted Output
    This replaces the broken _create_csv_from_data method
    """

    async def _create_csv_from_data(self, asset_data: Dict[str, pd.Series]) -> bool:
        """
        Create unified CSV file from downloaded asset data
        FIXED: Proper CSV formatting with separators and headers
        """
        try:
            logger.info(f"📝 Creating CSV file with {len(asset_data)} assets...")

            # Ensure data directory exists
            os.makedirs(os.path.dirname(self.csv_file), exist_ok=True)

            # Build a properly formatted DataFrame
            df = pd.DataFrame(asset_data)

            # Ensure the index is datetime and columns are properly named
            df.index.name = 'Date'

            # Sort by date and forward-fill any NaN values
            df = df.sort_index().ffill()

            # Remove any rows with all NaN values
            df = df.dropna(how='all')

            logger.info(f"📊 Final DataFrame shape: {df.shape}")
            logger.info(f"📅 Date range: {df.index.min()} to {df.index.max()}")
            logger.info(f"📈 Columns: {list(df.columns)}")

            # Save to CSV with proper formatting
            df.to_csv(
                self.csv_file,
                index=True,  # Include date index
                header=True,  # Include column headers
                date_format='%Y-%m-%d',  # Proper date format
                float_format='%.2f'  # 2 decimal places for numbers
            )

            # Verify the file was created correctly
            if os.path.exists(self.csv_file):
                # Read it back to verify structure
                test_df = pd.read_csv(self.csv_file, index_col=0, parse_dates=True)
                logger.info(f"✅ CSV verification: {test_df.shape} rows/cols")
                logger.info(f"📋 Sample data:\n{test_df.head(3)}")

                logger.info(f"🎉 CSV file created successfully: {self.csv_file}")
                return True
            else:
                logger.error(f"❌ CSV file was not created: {self.csv_file}")
                return False

        except Exception as e:
            logger.error(f"❌ Error creating CSV file: {e}")
            logger.error(f"📊 Asset data keys: {list(asset_data.keys())}")
            for asset, series in asset_data.items():
                logger.error(
                    f"  {asset}: {type(series)}, length: {len(series) if hasattr(series, '__len__') else 'N/A'}")
            return False

    # BONUS: Quick test to verify your current CSV
    def analyze_broken_csv(file_path: str):
        """Analyze the structure of the broken CSV file"""
        try:
            # Try to read the broken file
            with open(file_path, 'r') as f:
                first_few_lines = [f.readline().strip() for _ in range(5)]

            print("🔍 Current CSV structure:")
            for i, line in enumerate(first_few_lines):
                print(f"  Line {i + 1}: {line[:100]}...")

            # Try pandas read
            try:
                df = pd.read_csv(file_path)
                print(f"📊 Pandas interpretation: {df.shape} shape")
                print(f"📋 Columns: {list(df.columns)}")
            except Exception as e:
                print(f"❌ Pandas can't read it: {e}")

        except Exception as e:
            print(f"❌ Cannot analyze file: {e}")

    # USAGE EXAMPLE:
    # analyze_broken_csv("data/historical_data_1.csv")
    async def _fetch_portfolio_data(self, assets: List[str]) -> Dict[str, AssetDataResult]:
        """Internal method to fetch portfolio data (same as before)"""
        results = {}

        # Fetch data concurrently for all assets
        tasks = []
        for asset in assets:
            task = self._get_hybrid_asset_data(asset)
            tasks.append(task)

        # Execute all tasks concurrently
        asset_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(asset_results):
            asset = assets[i]
            if isinstance(result, Exception):
                logger.error(f"❌ Error fetching {asset}: {result}")
                results[asset] = self._get_fallback_asset_data(asset)
            else:
                results[asset] = result

        return results

    async def _get_hybrid_asset_data(self, asset: str) -> AssetDataResult:
        """
        Get hybrid data for single asset
        Strategy: Historical from CSV + Live price for today
        """
        logger.info(f"📊 Getting hybrid data for {asset}")

        try:
            # Step 1: Get historical data from CSV
            historical_data = await self._get_historical_data_from_csv(asset)

            # Step 2: Get live price for today
            live_price = await self._get_live_price_today(asset)

            # Step 3: Combine historical + live
            if historical_data and live_price:
                # Update historical data with today's live price
                combined_data = historical_data
                combined_data.current_price = live_price
                combined_data.current_stats['current_price'] = live_price
                combined_data.data_source = "hybrid_auto_downloaded_csv_plus_live_today"
                combined_data.last_update = datetime.now().isoformat()

                logger.info(f"✅ {asset}: Hybrid data (Auto-downloaded CSV + Live ${live_price:,.2f})")
                return combined_data

            elif historical_data:
                # Historical only (no live data available)
                logger.info(f"⚠️ {asset}: Historical data only (live unavailable)")
                historical_data.data_source = "auto_downloaded_csv_only"
                return historical_data

            elif live_price:
                # Live only (no historical data) - calculate basic metrics
                logger.info(f"⚠️ {asset}: Live data only (no CSV)")
                return self._create_live_only_result(asset, live_price)

            else:
                # Complete fallback
                logger.warning(f"❌ {asset}: Using complete fallback")
                return self._get_fallback_asset_data(asset)

        except Exception as e:
            logger.error(f"❌ Error in hybrid data fetch for {asset}: {e}")
            return self._get_fallback_asset_data(asset)

    async def _get_historical_data_from_csv(self, asset: str) -> Optional[AssetDataResult]:
        """Get historical data from CSV file (same as fixed version)"""
        try:
            if not self.csv_file.exists():
                logger.info(f"📂 CSV file not found: {self.csv_file}")
                return None

            logger.info(f"📊 Reading CSV data for {asset}...")

            # Read CSV in executor
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, self._read_csv_file)

            if df is None:
                logger.error(f"❌ Failed to read CSV file")
                return None

            if asset not in df.columns:
                logger.info(f"❌ {asset} not in CSV columns: {list(df.columns)}")
                return None

            # Get price series
            prices = df[asset].dropna()

            if len(prices) == 0:
                logger.warning(f"❌ No price data for {asset} in CSV")
                return None

            logger.info(f"📊 CSV data for {asset}: {len(prices)} points")

            # Calculate comprehensive metrics from historical data
            result = await self._calculate_metrics_from_prices(prices, asset, "auto_downloaded_csv_historical")
            return result

        except Exception as e:
            logger.error(f"❌ Error reading CSV for {asset}: {e}")
            return None

    def _read_csv_file(self) -> Optional[pd.DataFrame]:
        """Synchronous CSV reading function for executor"""
        try:
            # Try different CSV reading approaches
            try:
                # First try with index_col=0 (date column)
                df = pd.read_csv(self.csv_file, index_col=0, parse_dates=True)
                logger.info(f"✅ CSV loaded with date index: {df.shape}")
                return df
            except:
                # Fallback: No index column
                df = pd.read_csv(self.csv_file)
                logger.info(f"✅ CSV loaded without index: {df.shape}")
                return df

        except Exception as e:
            logger.error(f"❌ Failed to read CSV: {e}")
            return None

    async def _get_live_price_today(self, asset: str) -> Optional[float]:
        """Get live price for today only (same as before)"""
        try:
            # Check cache first (expires daily)
            today = datetime.now().date()

            if self.live_cache_date == today and asset in self.live_data_cache:
                logger.info(f"💾 Using cached live price for {asset}")
                return self.live_data_cache[asset]

            # Clear cache if new day
            if self.live_cache_date != today:
                self.live_data_cache.clear()
                self.live_cache_date = today

            # Get symbol
            symbol = self.asset_symbols.get(asset)
            if not symbol:
                logger.error(f"❌ No symbol mapping for {asset}")
                return None

            logger.info(f"⚡ Fetching live price for {asset} ({symbol})")

            # Fetch live price in executor
            loop = asyncio.get_event_loop()
            live_price = await loop.run_in_executor(None, self._fetch_yfinance_current_price, symbol)

            if live_price:
                # Cache for today
                self.live_data_cache[asset] = live_price
                logger.info(f"✅ Live price for {asset}: ${live_price:,.2f}")
                return live_price
            else:
                logger.warning(f"⚠️ No live price available for {asset}")
                return None

        except Exception as e:
            logger.error(f"❌ Error fetching live price for {asset}: {e}")
            return None

    def _fetch_yfinance_current_price(self, symbol: str) -> Optional[float]:
        """Fetch current price from YFinance (same as before)"""
        try:
            ticker = yf.Ticker(symbol)

            # Try to get current price from info first
            try:
                info = ticker.info
                current_price = info.get('currentPrice') or info.get('regularMarketPrice')
                if current_price:
                    return float(current_price)
            except:
                pass

            # Fallback: Get latest price from recent history
            hist = ticker.history(period="1d", interval="1m")

            if not hist.empty:
                latest_price = hist['Close'].iloc[-1]
                return float(latest_price)

            # Final fallback: Get daily close
            hist = ticker.history(period="2d")
            if not hist.empty:
                latest_price = hist['Close'].iloc[-1]
                return float(latest_price)

            return None

        except Exception as e:
            logger.error(f"❌ YFinance fetch failed for {symbol}: {e}")
            return None

    async def _calculate_metrics_from_prices(self, prices: pd.Series, asset_name: str, data_source: str) -> AssetDataResult:
        """Calculate comprehensive metrics from price series (same as before)"""
        try:
            current_price = float(prices.iloc[-1])

            logger.info(f"📊 Calculating metrics for {asset_name} from {data_source}")

            # Calculate returns with safe fallbacks
            returns_1m = self._safe_return_calc(prices, 21, "1.5%")
            returns_3m = self._safe_return_calc(prices, 63, "4.2%")
            returns_6m = self._safe_return_calc(prices, 126, "8.5%")
            returns_1y = self._safe_return_calc(prices, 252, "12.0%")
            returns_5y = self._safe_return_calc(prices, 252 * 5, "10.0%")
            returns_10y = self._safe_return_calc(prices, 252 * 10, "9.0%")

            # Calculate risk metrics
            volatility = self._safe_volatility_calc(prices, "18.0%")
            max_drawdown = self._safe_drawdown_calc(prices, "-22.0%")
            sharpe_ratio = self._safe_sharpe_calc(prices, "0.85")
            var_95 = self._safe_var_calc(prices, "-2.3%")

            # Technical indicators
            sma_50 = self._safe_sma_calc(prices, 50)
            sma_200 = self._safe_sma_calc(prices, 200)

            # Best available return for avg_annual
            avg_annual = returns_5y if returns_5y != "N/A" else (returns_1y if returns_1y != "N/A" else "10.0%")

            return AssetDataResult(
                asset_name=asset_name,
                current_price=current_price,
                data_source=data_source,
                last_update=datetime.now().isoformat(),
                historical_returns={
                    '1_month': returns_1m,
                    '3_months': returns_3m,
                    '6_months': returns_6m,
                    '1_year': returns_1y,
                    '5_years_avg': returns_5y,
                    '10_years_avg': returns_10y
                },
                risk_metrics={
                    'volatility': volatility,
                    'max_drawdown': max_drawdown,
                    'sharpe_ratio': sharpe_ratio,
                    'var_95': var_95
                },
                current_stats={
                    'current_price': current_price,
                    'sma_50': sma_50,
                    'sma_200': sma_200,
                    'avg_annual_return': avg_annual
                },
                data_quality={
                    'data_points': len(prices),
                    'data_start': str(prices.index[0]) if len(prices) > 0 else None,
                    'data_end': str(prices.index[-1]) if len(prices) > 0 else None,
                    'years_of_data': len(prices) / 252,
                    'completeness': min(len(prices) / (252 * 10), 1.0)
                }
            )

        except Exception as e:
            logger.error(f"❌ Error calculating metrics for {asset_name}: {e}")
            return self._get_fallback_asset_data(asset_name)

    # Include all the safe calculation methods (same as before)
    def _safe_return_calc(self, prices: pd.Series, days: int, fallback: str) -> str:
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
        except:
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

    def _create_live_only_result(self, asset: str, live_price: float) -> AssetDataResult:
        """Create result with live price only and estimated metrics"""
        logger.info(f"📊 Creating live-only result for {asset}: ${live_price:,.2f}")

        estimates = self._get_asset_estimates(asset)

        return AssetDataResult(
            asset_name=asset,
            current_price=live_price,
            data_source="live_price_only_today",
            last_update=datetime.now().isoformat(),
            historical_returns=estimates['returns'],
            risk_metrics=estimates['risk'],
            current_stats={
                'current_price': live_price,
                'sma_50': live_price * 0.98,
                'sma_200': live_price * 0.95,
                'avg_annual_return': estimates['returns']['1_year']
            },
            data_quality={
                'data_points': 1,
                'data_start': datetime.now().date().isoformat(),
                'data_end': datetime.now().date().isoformat(),
                'years_of_data': 0.004,
                'completeness': 0.1
            }
        )

    def _get_fallback_asset_data(self, asset: str) -> AssetDataResult:
        """Get complete fallback data for asset (same as your existing defaults)"""
        logger.warning(f"⚠️ Using fallback data for {asset}")

        estimates = self._get_asset_estimates(asset)

        return AssetDataResult(
            asset_name=asset,
            current_price=estimates['price'],
            data_source="complete_fallback",
            last_update=datetime.now().isoformat(),
            historical_returns=estimates['returns'],
            risk_metrics=estimates['risk'],
            current_stats={
                'current_price': estimates['price'],
                'sma_50': estimates['price'] * 0.98,
                'sma_200': estimates['price'] * 0.95,
                'avg_annual_return': estimates['returns']['1_year']
            },
            data_quality={
                'data_points': 0,
                'data_start': None,
                'data_end': None,
                'years_of_data': 0,
                'completeness': 0
            }
        )

    def _get_asset_estimates(self, asset: str) -> Dict[str, Any]:
        """Get reasonable estimates for asset (same as your existing defaults)"""
        estimates = {
            'Equities': {
                'price': 25000,
                'returns': {'1_month': "2.1%", '3_months': "5.2%", '6_months': "8.5%", '1_year': "15.0%", '5_years_avg': "12.0%", '10_years_avg': "11.0%"},
                'risk': {'volatility': "18.0%", 'max_drawdown': "-22.0%", 'sharpe_ratio': "0.95", 'var_95': "-2.1%"}
            },
            'Gold': {
                'price': 65000,
                'returns': {'1_month': "1.5%", '3_months': "4.2%", '6_months': "12.5%", '1_year': "8.0%", '5_years_avg': "10.0%", '10_years_avg': "8.5%"},
                'risk': {'volatility': "15.0%", 'max_drawdown': "-18.0%", 'sharpe_ratio': "1.05", 'var_95': "-1.8%"}
            },
            'Bitcoin': {
                'price': 4500000,
                'returns': {'1_month': "-5.2%", '3_months': "15.8%", '6_months': "45.2%", '1_year': "45.0%", '5_years_avg': "80.0%", '10_years_avg': "120.0%"},
                'risk': {'volatility': "65.0%", 'max_drawdown': "-75.0%", 'sharpe_ratio': "0.85", 'var_95': "-4.8%"}
            },
            'REITs': {
                'price': 180,
                'returns': {'1_month': "1.8%", '3_months': "4.5%", '6_months': "8.2%", '1_year': "12.0%", '5_years_avg': "15.0%", '10_years_avg': "13.5%"},
                'risk': {'volatility': "22.0%", 'max_drawdown': "-28.0%", 'sharpe_ratio': "0.88", 'var_95': "-2.8%"}
            }
        }

        return estimates.get(asset, estimates['Equities'])

    def _generate_request_hash(self, assets: List[str], request_id: str = None) -> str:
        """Generate hash for request deduplication"""
        hash_input = f"{sorted(assets)}_{request_id}_{datetime.now().date()}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    async def get_live_data_for_asset(self, asset: str) -> Dict[str, Any]:
        """Get live data for single asset"""
        result = await self._get_hybrid_asset_data(asset)
        return result.__dict__

    async def force_refresh_all(self) -> Dict[str, Any]:
        """Force refresh all cached data"""
        logger.info("🔄 Force refreshing auto-downloading hybrid system")

        # Clear live data cache
        self.live_data_cache.clear()
        self.live_cache_date = None

        # Clear active requests
        async with self.request_lock:
            self.active_requests.clear()

        # Force re-download CSV if needed
        if not self.csv_file.exists():
            await self._ensure_csv_exists()

        # Force refresh all assets
        assets = list(self.asset_symbols.keys())
        results = await self._fetch_portfolio_data(assets)

        logger.info(f"✅ Force refresh completed: {len(results)} assets")
        return {asset: result.__dict__ for asset, result in results.items()}

    def get_system_status(self) -> Dict[str, Any]:
        """Get auto-downloading hybrid system status"""
        today = datetime.now().date()

        return {
            "system_type": "auto_downloading_hybrid_data_manager",
            "strategy": "auto_download_historical_plus_live_pricing",
            "historical_source": {
                "type": "auto_downloaded_csv_file",
                "path": str(self.csv_file),
                "exists": self.csv_file.exists(),
                "size_mb": round(self.csv_file.stat().st_size / 1024 / 1024, 2) if self.csv_file.exists() else 0,
                "auto_download_enabled": True
            },
            "live_source": {
                "type": "yfinance_api",
                "symbols": self.asset_symbols,
                "cache_date": str(self.live_cache_date) if self.live_cache_date else None,
                "cached_assets": len(self.live_data_cache)
            },
            "performance": {
                "active_requests": len(self.active_requests),
                "cache_enabled": True,
                "concurrent_support": True,
                "request_deduplication": True,
                "auto_initialization": True
            },
            "data_freshness": {
                "historical_data": "Auto-downloaded from YFinance (up to 10 years)",
                "live_data": "Real-time for today (day X)",
                "cache_expiry": "Daily (end of day)"
            },
            "download_status": {
                "csv_download_in_progress": self.csv_download_in_progress,
                "first_run_auto_download": not self.csv_file.exists()
            },
            "timestamp": datetime.now().isoformat(),
            "status": "operational"
        }


# Backward compatibility alias
HybridDataManager = AutoDownloadingHybridDataManager