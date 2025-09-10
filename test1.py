import sys

sys.path.append('.')

from historical_analyzer import EnhancedHistoricalAnalyzer
import json


def test_complete_calculations():
    """Test that we get NO N/A values"""
    print("🧪 TESTING COMPLETE CALCULATIONS (NO N/A)")
    print("=" * 50)

    analyzer = EnhancedHistoricalAnalyzer()

    portfolios = {
        "current": {"Equities": 30, "Gold": 40, "Bitcoin": 20, "REITs": 10},
        "optimized": {"Equities": 35, "Gold": 35, "Bitcoin": 20, "REITs": 10}
    }

    results = analyzer.calculate_all_metrics(portfolios)

    print(f"✅ Results for {len(results)} assets:")

    # Check for N/A values
    na_count = 0
    total_fields = 0

    for asset, data in results.items():
        print(f"\n📊 {asset}:")

        # Check historical returns
        hist_returns = data.get('historical_returns', {})
        for period, value in hist_returns.items():
            total_fields += 1
            if value == "N/A":
                na_count += 1
                print(f"   ❌ {period}: {value}")
            else:
                print(f"   ✅ {period}: {value}")

        # Check risk metrics
        risk_metrics = data.get('risk_metrics', {})
        for metric, value in risk_metrics.items():
            total_fields += 1
            if value == "N/A":
                na_count += 1
                print(f"   ❌ {metric}: {value}")
            else:
                print(f"   ✅ {metric}: {value}")

        # Check current stats
        current_stats = data.get('current_stats', {})
        price = current_stats.get('current_price', 0)
        avg_return = current_stats.get('avg_annual_return', 'N/A')

        print(f"   💰 Current price: ${price:,.2f}")
        print(f"   📈 Avg annual return: {avg_return}")
        print(f"   🔗 Data source: {data.get('data_source', 'unknown')}")

    print(f"\n📋 SUMMARY:")
    print(f"   Total fields checked: {total_fields}")
    print(f"   N/A values found: {na_count}")

    if na_count == 0:
        print("   🎉 SUCCESS: NO N/A VALUES!")
    else:
        print(f"   ⚠️ ISSUE: {na_count} N/A values still present")

    return results, na_count == 0


if __name__ == "__main__":
    results, success = test_complete_calculations()
