#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

double nan_value() {
    return std::numeric_limits<double>::quiet_NaN();
}

// Most indicators need a positive lookback window.
void require_period(int period, const char* name) {
    if (period <= 0) {
        throw std::invalid_argument(std::string(name) + " period must be positive");
    }
}

// Some indicators use parallel arrays, so their lengths must match.
void require_same_size(const std::vector<double>& a, const std::vector<double>& b, const char* name) {
    if (a.size() != b.size()) {
        throw std::invalid_argument(std::string(name) + " inputs must have the same length");
    }
}

}  // namespace

struct MACDResult {
    std::vector<double> macd;
    std::vector<double> signal;
    std::vector<double> histogram;
};

struct BollingerResult {
    std::vector<double> upper;
    std::vector<double> middle;
    std::vector<double> lower;
};

// RSI uses Wilder smoothing:
// seed average gain/loss first, then smooth the next points.
std::vector<double> rsi(const std::vector<double>& prices, int period) {
    require_period(period, "RSI");
    if (prices.size() < static_cast<size_t>(period + 1)) {
        throw std::invalid_argument("not enough prices for RSI");
    }

    std::vector<double> result(prices.size(), nan_value());
    double avg_gain = 0.0;
    double avg_loss = 0.0;

    for (int i = 1; i <= period; ++i) {
        const double change = prices[i] - prices[i - 1];
        if (change >= 0.0) {
            avg_gain += change;
        } else {
            avg_loss -= change;
        }
    }

    avg_gain /= period;
    avg_loss /= period;
    result[period] = avg_loss == 0.0 ? 100.0 : 100.0 - (100.0 / (1.0 + avg_gain / avg_loss));

    for (size_t i = static_cast<size_t>(period + 1); i < prices.size(); ++i) {
        const double change = prices[i] - prices[i - 1];
        const double gain = change > 0.0 ? change : 0.0;
        const double loss = change < 0.0 ? -change : 0.0;

        avg_gain = ((avg_gain * (period - 1)) + gain) / period;
        avg_loss = ((avg_loss * (period - 1)) + loss) / period;
        result[i] = avg_loss == 0.0 ? 100.0 : 100.0 - (100.0 / (1.0 + avg_gain / avg_loss));
    }

    return result;
}

// SMA is the rolling arithmetic mean over a fixed window.
std::vector<double> sma(const std::vector<double>& prices, int period) {
    require_period(period, "SMA");
    if (prices.size() < static_cast<size_t>(period)) {
        throw std::invalid_argument("not enough prices for SMA");
    }

    std::vector<double> result(prices.size(), nan_value());
    double window_sum = 0.0;

    for (size_t i = 0; i < prices.size(); ++i) {
        window_sum += prices[i];
        if (i >= static_cast<size_t>(period)) {
            window_sum -= prices[i - period];
        }
        if (i + 1 >= static_cast<size_t>(period)) {
            result[i] = window_sum / period;
        }
    }

    return result;
}

// EMA starts from an SMA seed and then gives more weight to new prices.
std::vector<double> ema(const std::vector<double>& prices, int period) {
    require_period(period, "EMA");
    if (prices.size() < static_cast<size_t>(period)) {
        throw std::invalid_argument("not enough prices for EMA");
    }

    std::vector<double> result(prices.size(), nan_value());
    double sum = 0.0;
    for (int i = 0; i < period; ++i) {
        sum += prices[i];
    }

    const size_t seed_index = static_cast<size_t>(period - 1);
    result[seed_index] = sum / period;

    const double multiplier = 2.0 / (period + 1.0);
    for (size_t i = seed_index + 1; i < prices.size(); ++i) {
        result[i] = ((prices[i] - result[i - 1]) * multiplier) + result[i - 1];
    }

    return result;
}

// MACD is fast EMA minus slow EMA, plus a signal EMA and histogram.
MACDResult macd(const std::vector<double>& prices, int fast, int slow, int signal_period) {
    require_period(fast, "MACD fast");
    require_period(slow, "MACD slow");
    require_period(signal_period, "MACD signal");
    if (fast >= slow) {
        throw std::invalid_argument("MACD fast period must be less than slow period");
    }
    if (prices.size() < static_cast<size_t>(slow + signal_period)) {
        throw std::invalid_argument("not enough prices for MACD");
    }

    const std::vector<double> fast_ema = ema(prices, fast);
    const std::vector<double> slow_ema = ema(prices, slow);

    MACDResult result;
    result.macd.assign(prices.size(), nan_value());
    result.signal.assign(prices.size(), nan_value());
    result.histogram.assign(prices.size(), nan_value());

    for (size_t i = static_cast<size_t>(slow - 1); i < prices.size(); ++i) {
        result.macd[i] = fast_ema[i] - slow_ema[i];
    }

    double seed_sum = 0.0;
    const size_t signal_seed = static_cast<size_t>(slow - 1 + signal_period - 1);
    for (size_t i = static_cast<size_t>(slow - 1); i <= signal_seed; ++i) {
        seed_sum += result.macd[i];
    }
    result.signal[signal_seed] = seed_sum / signal_period;

    const double multiplier = 2.0 / (signal_period + 1.0);
    for (size_t i = signal_seed + 1; i < prices.size(); ++i) {
        result.signal[i] = ((result.macd[i] - result.signal[i - 1]) * multiplier) + result.signal[i - 1];
    }

    for (size_t i = signal_seed; i < prices.size(); ++i) {
        result.histogram[i] = result.macd[i] - result.signal[i];
    }

    return result;
}

// ATR measures movement size, including overnight gaps from the prior close.
std::vector<double> atr(
    const std::vector<double>& highs,
    const std::vector<double>& lows,
    const std::vector<double>& closes,
    int period
) {
    require_period(period, "ATR");
    require_same_size(highs, lows, "ATR");
    require_same_size(highs, closes, "ATR");
    if (closes.size() < static_cast<size_t>(period)) {
        throw std::invalid_argument("not enough prices for ATR");
    }

    std::vector<double> true_range(closes.size(), 0.0);
    true_range[0] = highs[0] - lows[0];

    for (size_t i = 1; i < closes.size(); ++i) {
        const double high_low = highs[i] - lows[i];
        const double high_close = std::abs(highs[i] - closes[i - 1]);
        const double low_close = std::abs(lows[i] - closes[i - 1]);
        true_range[i] = std::max(high_low, std::max(high_close, low_close));
    }

    std::vector<double> result(closes.size(), nan_value());
    double sum = 0.0;
    for (int i = 0; i < period; ++i) {
        sum += true_range[i];
    }

    const size_t seed_index = static_cast<size_t>(period - 1);
    result[seed_index] = sum / period;

    for (size_t i = seed_index + 1; i < closes.size(); ++i) {
        result[i] = ((result[i - 1] * (period - 1)) + true_range[i]) / period;
    }

    return result;
}

// Bollinger bands use SMA for the middle line and standard deviation for the outer bands.
BollingerResult bollinger(const std::vector<double>& prices, int period, double stddev_multiplier) {
    require_period(period, "Bollinger");
    if (prices.size() < static_cast<size_t>(period)) {
        throw std::invalid_argument("not enough prices for Bollinger Bands");
    }

    BollingerResult result;
    result.upper.assign(prices.size(), nan_value());
    result.middle = sma(prices, period);
    result.lower.assign(prices.size(), nan_value());

    for (size_t i = static_cast<size_t>(period - 1); i < prices.size(); ++i) {
        const double mean = result.middle[i];
        double variance = 0.0;

        for (size_t j = i + 1 - period; j <= i; ++j) {
            const double diff = prices[j] - mean;
            variance += diff * diff;
        }

        const double stddev = std::sqrt(variance / period);
        result.upper[i] = mean + (stddev_multiplier * stddev);
        result.lower[i] = mean - (stddev_multiplier * stddev);
    }

    return result;
}

// OBV adds or subtracts volume depending on whether close moved up or down.
std::vector<double> obv(const std::vector<double>& closes, const std::vector<double>& volumes) {
    require_same_size(closes, volumes, "OBV");
    if (closes.empty()) {
        throw std::invalid_argument("not enough prices for OBV");
    }

    std::vector<double> result(closes.size(), 0.0);
    for (size_t i = 1; i < closes.size(); ++i) {
        if (closes[i] > closes[i - 1]) {
            result[i] = result[i - 1] + volumes[i];
        } else if (closes[i] < closes[i - 1]) {
            result[i] = result[i - 1] - volumes[i];
        } else {
            result[i] = result[i - 1];
        }
    }

    return result;
}
