import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from config import Config

logger = logging.getLogger(__name__)


class HistoricalAnalyzer:
    def __init__(self, csv_path=None):
        self.csv_path = csv_path or Config.HISTORICAL_CSV_PATH
        self.data = None
        self.load_data()

    def load_data(self):
        """Load historical data from CSV file"""
        try:
            self.data = pd.read_csv(self.csv_path)
            # Assuming first column is Date
            if 'Date' in self.data.columns:
                self.data['Date'] = pd.to_datetime(self.data['Date'])
                self.data.set_index('Date', inplace=True)
            else:
                # If no Date column, assume first column is date
                self.data.iloc[:, 0] = pd.to_datetime(self.data.iloc[:, 0])
                self.data.set_index(self.data.columns[0], inplace=True)

            logger.info(f"Loaded historical data: {self.data.shape}")
            logger.info(f"Columns: {list(self.data.columns)}")
            logger.info(f"Date range: {self.data.index[0]} to {self.data.index[-1]}")

        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            # Create sample data for testing
            self.create_sample_data()

    def create_sample_data(self):
        """Create sample historical data for testing"""
        logger.warning("Creating sample historical data for testing")

        # Create 10 years of sample data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * 10)
        dates = pd.date_range(start=start_date, end=end_date, freq='D')

        # Generate sample prices with realistic patterns
        np.random.seed(42)

        # Sample data for each asset
        equities_prices = [15000]  # Starting NIFTY price
        gold_prices = [50000]  # Starting Gold price (INR/10g)
        bitcoin_prices = [500000]  # Starting Bitcoin price (INR)
        reit_prices = [100]  # Starting REIT price

        for i in range(1, len(dates)):
            # Generate realistic returns
            equities_prices.append(equities_prices[-1] * (1 + np.random.normal(0.0008, 0.015)))
            gold_prices.append(gold_prices[-1] * (1 + np.random.normal(0.0005, 0.012)))
            bitcoin_prices.append(bitcoin_prices[-1] * (1 + np.random.normal(0.002, 0.04)))
            reit_prices.append(reit_prices[-1] * (1 + np.random.normal(0.0006, 0.018)))

        self.data = pd.DataFrame({
            'Equities': equities_prices,
            'Gold': gold_prices,
            'Bitcoin': bitcoin_prices,
            'REITs': reit_prices
        }, index=dates)

        logger.info(f"Created sample data: {self.data.shape}")

    def calculate_returns_and_metrics(self):
        """Calculate comprehensive metrics for all assets"""
        if self.data is None:
            return {}

        results = {}

        for asset in self.data.columns:
            try:
                results[asset] = self._calculate_asset_metrics(self.data[asset])
                logger.info(f"Calculated metrics for {asset}")
            except Exception as e:
                logger.error(f"Error calculating metrics for {asset}: {e}")
                results[asset] = self._get_default_metrics()

        return results

    def _calculate_asset_metrics(self, price_series):
        """Calculate comprehensive metrics for single asset"""
        # Calculate returns
        returns = price_series.pct_change().dropna()

        # Get date ranges
        end_date = price_series.index[-1]
        one_year_ago = end_date - timedelta(days=365)
        five_years_ago = end_date - timedelta(days=365 * 5)
        ten_years_ago = end_date - timedelta(days=365 * 10)

        # Calculate period returns
        try:
            # Find closest dates
            one_year_price = price_series[price_series.index >= one_year_ago].iloc[0]
            current_price = price_series.iloc[-1]
            one_year_return = ((current_price / one_year_price) - 1) * 100

            five_year_price = price_series[price_series.index >= five_years_ago].iloc[0]
            five_year_return = ((current_price / five_year_price) ** (1 / 5) - 1) * 100

            ten_year_price = price_series[price_series.index >= ten_years_ago].iloc[0]
            ten_year_return = ((current_price / ten_year_price) ** (1 / 10) - 1) * 100

        except (IndexError, KeyError):
            # Fallback calculations
            one_year_return = returns.tail(252).mean() * 252 * 100 if len(returns) >= 252 else 0
            five_year_return = returns.mean() * 252 * 100
            ten_year_return = returns.mean() * 252 * 100

        # Risk metrics
        volatility = returns.std() * np.sqrt(252) * 100  # Annualized volatility

        # Max drawdown
        rolling_max = price_series.expanding().max()
        drawdown = (price_series - rolling_max) / rolling_max
        max_drawdown = drawdown.min() * 100

        # VaR (95%) - daily
        var_95 = np.percentile(returns, 5) * 100 if len(returns) > 0 else 0

        # Additional metrics
        avg_return = returns.mean() * 252 * 100  # Annualized
        sharpe_ratio = avg_return / volatility if volatility > 0 else 0

        return {
            "historical_returns": {
                "1_year": f"{one_year_return:.1f}%",
                "5_years_avg": f"{five_year_return:.1f}%",
                "10_years_avg": f"{ten_year_return:.1f}%"
            },
            "risk_metrics": {
                "volatility": f"{volatility:.1f}%",
                "max_drawdown": f"{max_drawdown:.1f}%",
                "var_95": f"{var_95:.1f}%",
                "sharpe_ratio": f"{sharpe_ratio:.2f}"
            },
            "current_stats": {
                "current_price": float(price_series.iloc[-1]),
                "avg_annual_return": f"{avg_return:.1f}%"
            }
        }

    def _get_default_metrics(self):
        """Return default metrics when calculation fails"""
        return {
            "historical_returns": {
                "1_year": "0.0%",
                "5_years_avg": "0.0%",
                "10_years_avg": "0.0%"
            },
            "risk_metrics": {
                "volatility": "0.0%",
                "max_drawdown": "0.0%",
                "var_95": "0.0%",
                "sharpe_ratio": "0.00"
            },
            "current_stats": {
                "current_price": 0.0,
                "avg_annual_return": "0.0%"
            }
        }


# Test function
if __name__ == "__main__":
    analyzer = HistoricalAnalyzer()
    metrics = analyzer.calculate_returns_and_metrics()

    print("Historical Analysis Test:")
    for asset, data in metrics.items():
        print(f"\n{asset}:")
        print(f"  1Y Return: {data['historical_returns']['1_year']}")
        print(f"  5Y Avg: {data['historical_returns']['5_years_avg']}")
        print(f"  Volatility: {data['risk_metrics']['volatility']}")
